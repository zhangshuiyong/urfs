use std::collections::HashMap;
use std::fmt::{Debug};
use std::path::{Path, PathBuf};
use anyhow::{Error, Ok, Result};
use std::io::SeekFrom;
use tokio::fs::File;
use tokio::io::{AsyncReadExt,AsyncSeekExt};

use reqwest::{multipart, Client, ClientBuilder};
use serde::{Deserialize, Serialize};
use tokio::sync::{mpsc, Semaphore, Notify};
use tokio::time::Duration;
use std::sync::Arc;

use nydus_storage::{RAFS_MAX_CHUNK_SIZE,RAFS_MAX_CHUNKS_PER_BLOB};

pub const HTTP_CONN_POOL_DEFAULT_SIZE: usize = 10;
pub const HTTP_CONN_RECYCLE_TIMEOUT: u64 = 60;
pub const CHUNK_UPLOADER_MAX_CONCURRENCY: usize = 4;

use std::fmt;

#[derive(Debug)]
enum UploadError {
    FileSizeLimitExceeded,
    RequestErr(String),
    ResponseErr(String),
}

enum DataSetStatus{
    Uploading,
    UploadChunkSuccess,
    AsyncProcessing,
    Success,
    UnKnown,
}

enum UploadMode{
    Source,
    Meta,
    Ephemeral,
    Chunk,
    ChunkEnd,
}

impl fmt::Display for UploadMode {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            Self::Source  => write!(f, "{}", '0'),
            Self::Meta => write!(f, "{}", '1'),
            Self::Ephemeral => write!(f, "{}", '2'),
            Self::Chunk => write!(f, "{}", '3'),
            Self::ChunkEnd => write!(f, "{}", '4'),
        }
    }
}

/// get http client from connection pool
fn get_http_client() -> Result<Client> {

    let httpclient = ClientBuilder::new()
        .pool_idle_timeout(Duration::from_secs(HTTP_CONN_RECYCLE_TIMEOUT))
        .pool_max_idle_per_host(HTTP_CONN_POOL_DEFAULT_SIZE).build()?;

    Ok(httpclient)
}

//Support HTTP/HTTPS
pub struct DatasetUploader {
    upload_dataset_history: HashMap<String,DatasetMeta>,
    all_dataset_chunk_sema :Arc<Semaphore>,
}

impl DatasetUploader {
    pub fn new() -> Self {

        Self {
            upload_dataset_history: HashMap::new(),
            all_dataset_chunk_sema: Arc::new(Semaphore::new(CHUNK_UPLOADER_MAX_CONCURRENCY)),
        }
    }

    pub async fn upload(&self, dataset_meta_path: PathBuf,dataset_meta:DatasetMeta,dataset_blob_path:PathBuf,upload_endpoint: String) -> Result<()> {

        println!("dataset_meta_path: {:?}",dataset_meta_path);
        println!("dataset_blob_path: {:?}",dataset_blob_path);

        println!("dataset_meta info:{:?}",dataset_meta);
        println!("dataset_meta max size:{:?} TB",DatasetMeta::maxsize()?);

        //self.upload_meta(dataset_meta_path,dataset_meta.clone(),upload_endpoint.clone()).await?;

        self.upload_blob(dataset_blob_path,dataset_meta.clone(),upload_endpoint.clone()).await?;

        println!("upload dataset finish!!!");
        Ok(())
    }

    pub async fn upload_meta(&self, dataset_meta_path: PathBuf,dataset_meta:DatasetMeta,upload_endpoint: String) -> Result<()> {

        println!("dataset_meta_path: {:?}",dataset_meta_path);

        println!("dataset_meta info:{:?}",dataset_meta);

        let mut meta_file= File::open(dataset_meta_path.as_path()).await?;

        //ToDo: should be remove!!!
        let fi = meta_file.metadata().await?;

        println!("[upload_meta]: file meta {:?}",fi);

        let meta_file_name = dataset_meta_path.file_name().unwrap().to_str().unwrap().to_string();

        let mut contents:Vec<u8> = vec![];

        meta_file.read_to_end(&mut contents).await?;

        let file_part = multipart::Part::bytes(contents)
            .file_name(meta_file_name)
            .mime_str("application/octet-stream")?;

        let form = multipart::Form::new()
            .part("file", file_part)
            .text("mode",UploadMode::Meta.to_string())
            .text("dataset_id","xxx")
            .text("dataset_version_id","default")
            .text("digest",dataset_meta.id)
            .text("digester",dataset_meta.digester)
            .text("total_size",fi.len().to_string());

        let httpclient = get_http_client()?;

        let upload_meta_url = upload_endpoint + "/api/v1/file/upload";

        println!("[upload_meta]: upload meta url {:?}",upload_meta_url);

        let resp = httpclient
            .put(upload_meta_url)
            .multipart(form)
            .send().await?;

        let result = resp.text().await?;

        println!("upload dataset_meta finish, result:{}!!!",result);
        Ok(())
    }

    pub async fn upload_blob(&self, dataset_blob_path: PathBuf,dataset_meta:DatasetMeta,upload_endpoint: String) -> Result<()> {

        let all_dataset_chunk_sema = self.all_dataset_chunk_sema.clone();

        Self::create_blob_chunks_manager(all_dataset_chunk_sema,dataset_meta,dataset_blob_path,upload_endpoint).await?;

        Ok(())
    }

    pub async fn create_blob_chunks_manager(all_dataset_chunk_sema:Arc<Semaphore>,chunks_dataset:DatasetMeta,dataset_file_path: PathBuf,upload_endpoint: String) -> Result<()> {

        let mut chunks_manager = DatasetChunksManager::new(chunks_dataset,all_dataset_chunk_sema,upload_endpoint,dataset_file_path);

        //ToDo: check server to determine 3 state: upload_start/upload_finish/partial_upload!

        // let (chunk_pusher,mut chunk_getter) = mpsc::channel(100);
        // let (chunk_result_sender,mut chunk_result_collector) = mpsc::channel(100);
        // chunks_manager.create_data_chunk_producer(chunk_pusher,chunk_result_sender).await;
        // chunks_manager.create_data_chunk_consumer(chunk_getter).await;
        //
        // let mut rest_upload_size = chunks_manager.upload_dataset.compressed_size;
        // while let Some(chunk_result) = chunk_result_collector.recv().await {
        //     println!("[manager]: received chunk_result: {:?}", chunk_result);
        //
        //     //ToDo: process err
        //     if !chunk_result.status.is_none(){
        //         rest_upload_size -= chunk_result.uploaded_size;
        //     }
        //
        //     if rest_upload_size == 0 {
        //         println!("[manager]: upload dataset blob success !!!");
        //         break;
        //     }
        // }

        //ToDo: Check Upload Dataset blob Status
        //println!("[manager]: upload chunks finished, check upload status, rest_upload_size:{}!!!",rest_upload_size);

        //ToDo: process Result Error!!!
        let result = chunks_manager.merge_data_chunks().await;

        println!("[manager]: dataset sent upload chunk end cmd, result:{:?}!!!",result);

        Ok(())
    }
}

pub struct DatasetChunksManager{
    //all dataset contain many chunks, concurrency from all dataset should be limited
    all_dataset_chunk_sema: Arc<Semaphore>,
    //one dataset also contain many chunks, concurrency form one dataset also should be limited
    one_dataset_chunk_sema: Arc<Semaphore>,
    upload_endpoint: String,
    upload_file_path: PathBuf,
    upload_status: String,
    upload_dataset: DatasetMeta,
}

impl DatasetChunksManager {
    pub fn new(dataset_meta:DatasetMeta,all_dataset_sema:Arc<Semaphore>,endpoint:String,file_path:PathBuf) -> Self {

        let one_dataset_sema = Arc::new(Semaphore::new(CHUNK_UPLOADER_MAX_CONCURRENCY+1));
        //ToDo: process upload status
        Self {
            all_dataset_chunk_sema: all_dataset_sema,
            one_dataset_chunk_sema: one_dataset_sema,
            upload_endpoint: endpoint,
            upload_file_path: file_path,
            upload_dataset: dataset_meta,
            upload_status: String::from("initial"),
        }
    }

    async fn create_data_chunk_producer(&self,chunk_pusher:mpsc::Sender<DatasetChunk>,chunk_result_sender:mpsc::Sender<DatasetChunkResult>) {

        let upload_dataset = self.upload_dataset.clone();
        let all_dataset_chunk_sema = self.all_dataset_chunk_sema.clone();
        let one_dataset_chunk_sema = self.one_dataset_chunk_sema.clone();
        let dataset_compressed_size = upload_dataset.compressed_size;
        let dataset_chunk_size= self.upload_dataset.chunk_size;
        let upload_endpoint= self.upload_endpoint.clone();
        let upload_file_path= self.upload_file_path.clone();

        tokio::spawn(async move {

            let mut chunk_seek_start  = 0u64;
            let mut chunk_num  = 0u64;

            while chunk_seek_start <= dataset_compressed_size-1 {

                let dc = DatasetChunk::new(upload_dataset.clone(),
                                           chunk_num,
                                           chunk_seek_start,
                                           upload_endpoint.clone(),
                                           upload_file_path.clone(),
                                           all_dataset_chunk_sema.clone(),
                  one_dataset_chunk_sema.clone(),
                                           chunk_result_sender.clone()
                );

                if chunk_pusher.send(dc).await.is_err() {
                    error!("[chunk_producer]: chunks_chan_getter closed!!!");
                    return;
                }

                chunk_seek_start += dataset_chunk_size as u64;
                chunk_num += 1;
            }

            println!("[chunk_producer]: finish to push all DatasetChunk !!!");
        });

    }

    async fn create_data_chunk_consumer(&mut self,mut chunks_chan_getter:mpsc::Receiver<DatasetChunk>) {

        let create_chunk_task_sema = self.one_dataset_chunk_sema.clone();

        tokio::spawn(async move {

            while let Some(dc) = chunks_chan_getter.recv().await {

                let _create_chunk_task_permit = create_chunk_task_sema.acquire().await?;
                println!("create chunk task permit by one_dataset!");

                tokio::spawn(async move {

                    Self::create_upload_chunk_task(dc).await?;

                    Ok(())
                });
            }

            Ok(())
        });
    }

    async fn merge_data_chunks(&self) -> Result<()>{
        //ToDo: digester not ztd! shold be rename to urfs or else
        let form = multipart::Form::new()
            .text("mode",UploadMode::ChunkEnd.to_string())
            .text("dataset_id","xxx")
            .text("dataset_version_id","default")
            .text("digest",self.upload_dataset.id.clone())
            .text("digester","urfs")
            .text("total_size",self.upload_dataset.compressed_size.to_string())
            .text("chunk_size",self.upload_dataset.chunk_size.to_string());

        let httpclient = get_http_client()?;

        let upload_chunk_end_url = self.upload_endpoint.clone()+"/api/v1/file/upload";

        println!("[upload_chunk_task]: upload DataChunk End id:{:?} ,chunk_size:{:?} ,url {:?}",
                 self.upload_dataset.id,
                 self.upload_dataset.chunk_size,
                 upload_chunk_end_url);

        let result = httpclient
            .put(upload_chunk_end_url)
            .multipart(form)
            .send().await;

        if let Result::Ok(resp) = result {

            let resp_txt = resp.text().await?;

            println!("[upload_chunk_end]: upload DataChunk End Ok, resp: {:?}",resp_txt);

            Ok(())

        }else if let Some(err) = result.err() {

            error!("[upload_chunk_end]: upload DataChunk End err, dataset.id:{:?} err:{}!!!",self.upload_dataset.id,err);
            anyhow::bail!("upload DataChunk End err,dataset.id:{:?} err:{}!!!",self.upload_dataset.id,err)

        }else{

            error!("[upload_chunk_end]: upload DataChunk End err, dataset.id:{:?}!!!",self.upload_dataset.id);
            anyhow::bail!("upload DataChunk End err,dataset.id:{:?}!!!",self.upload_dataset.id)

        }
    }

    pub async fn create_upload_chunk_task(data_chunk:DatasetChunk) -> Result<()> {

        let _run_permit_by_one_dataset = data_chunk.one_dataset_sema.acquire().await?;
        println!("[upload_chunk_task]: upload DataChunk permit by one_dataset!");
        let _run_permit_by_all_dataset = data_chunk.all_dataset_sema.acquire().await?;
        println!("[upload_chunk_task]: upload DataChunk permit by all_dataset!!");

        println!("[upload_chunk_task]: ready to upload DataChunk:{:?}", data_chunk.chunk_seek_start);

        //println!("target_dataset_path:{:?},target_url:{:?}",target_dataset_path,target_url);

        let mut dataset_file= File::open(data_chunk.upload_file_path.as_path()).await?;

        let fi = dataset_file.metadata().await?;

        let dataset_file_name = data_chunk.upload_file_path.file_name().unwrap().to_str().unwrap().to_string();

        let _ = dataset_file.seek(SeekFrom::Start(data_chunk.chunk_seek_start)).await?;


        let mut content_length = data_chunk.dataset.chunk_size as usize;
        let mut contents = vec![0;content_length];
        let chunk_end = data_chunk.chunk_seek_start+data_chunk.dataset.chunk_size;
        if chunk_end > data_chunk.dataset.compressed_size {
            content_length = (data_chunk.dataset.compressed_size-data_chunk.chunk_seek_start) as usize;
            contents = vec![0;content_length];
        }
        println!("chunk_num:{} chunk_start:{} chunk_end:{} totalsize:{}",data_chunk.chunk_num,
                 data_chunk.chunk_seek_start,chunk_end,data_chunk.dataset.compressed_size);

        dataset_file.read(&mut contents).await?;

        let file_part = multipart::Part::bytes(contents)
            .file_name(dataset_file_name)
            .mime_str("application/octet-stream")?;

        let form = multipart::Form::new()
            .part("file", file_part)
            .text("mode",UploadMode::Chunk.to_string())
            .text("dataset_id","xxx")
            .text("dataset_version_id","default")
            .text("digest",data_chunk.dataset.id)
            .text("digester",data_chunk.dataset.digester)
            .text("total_size",data_chunk.dataset.compressed_size.to_string())
            .text("chunk_size",data_chunk.dataset.chunk_size.to_string())
            .text("chunk_start",data_chunk.chunk_seek_start.to_string())
            .text("chunk_num",data_chunk.chunk_num.to_string());

        let httpclient = get_http_client()?;

        let upload_chunk_url = data_chunk.upload_endpoint+"/api/v1/file/upload";

        println!("[upload_chunk_task]: upload DataChunk num:{:?} ,chunk_size:{:?} ,url {:?}",
                 data_chunk.chunk_num.to_string(),
                 data_chunk.dataset.chunk_size.to_string(),
                 upload_chunk_url);

        let result = httpclient
            .put(upload_chunk_url)
            .multipart(form)
            .send().await;

        if let Result::Ok(resp) = result {

            let resp_txt = resp.text().await?;

            println!("[upload_chunk_task]: upload DataChunk finish, resp: {:?}",resp_txt);

            //time::sleep(Duration::from_millis(20000)).await;

            data_chunk.upload_result_sender.send(DatasetChunkResult::new(
                data_chunk.chunk_num,
                data_chunk.chunk_seek_start,
                content_length as u64,
                Some(())
            )).await?;

            println!("[upload_chunk_task]: notify to chunk manager.");

            Ok(())

        }else if let Some(err) = result.err() {


            error!("[upload_chunk_task]: upload DataChunk err, chunk num:{:?} err:{}!!!",data_chunk.chunk_num.to_string(),err);

            data_chunk.upload_result_sender.send(DatasetChunkResult::new(
                data_chunk.chunk_num,
                data_chunk.chunk_seek_start,
                content_length as u64,
                None
            )).await?;

            anyhow::bail!("upload DataChunk err,chunk num:{} err:{}!!!",data_chunk.chunk_num.to_string(),err.to_string())
        }else{

            error!("[upload_chunk_task]: upload DataChunk err, chunk num:{:?}!!!",data_chunk.chunk_num.to_string());

            data_chunk.upload_result_sender.send(DatasetChunkResult::new(
                data_chunk.chunk_num,
                data_chunk.chunk_seek_start,
                content_length as u64,
                None
            )).await?;

            anyhow::bail!("upload ataChunk err,chunk num:{:?}!!!",data_chunk.chunk_num.to_string())

        }
    }
}

#[derive(Clone, Debug)]
pub struct DatasetChunk{
    dataset: DatasetMeta,
    chunk_num: u64,
    chunk_seek_start: u64,
    upload_endpoint: String,
    upload_file_path: PathBuf,
    upload_result_sender: mpsc::Sender<DatasetChunkResult>,
    //all dataset contain many chunks, concurrency from all dataset should be limited
    all_dataset_sema: Arc<Semaphore>,
    //one dataset also contain many chunks, concurrency form one dataset also should be limited
    one_dataset_sema: Arc<Semaphore>,
}

impl DatasetChunk {
    pub fn new(ds: DatasetMeta,
               num: u64,
               start:u64,
               endpoint:String,
               file_path: PathBuf,
               sema_permit_by_all_dataset:Arc<Semaphore>,
               sema_permit_by_one_dataset:Arc<Semaphore>,
               result_sender:mpsc::Sender<DatasetChunkResult>) -> Self {

        Self {
            dataset: ds,
            chunk_num: num,
            chunk_seek_start: start,
            upload_endpoint: endpoint,
            upload_file_path: file_path,
            upload_result_sender: result_sender,
            all_dataset_sema: sema_permit_by_all_dataset,
            one_dataset_sema: sema_permit_by_one_dataset,
        }
    }
}


#[derive(Clone, Debug)]
pub struct DatasetChunkResult{
    chunk_num: u64,
    chunk_seek_start: u64,
    uploaded_size: u64,
    status: Option<()>
}

impl DatasetChunkResult {
    pub fn new(num:u64, start:u64, size:u64,upload_status: Option<()>) -> Self {

        Self {
            chunk_num: num,
            chunk_seek_start: start,
            uploaded_size: size,
            status: upload_status
        }
    }
}

#[derive(Clone, Debug, Default,Serialize,Deserialize)]
pub struct DatasetMeta {
    /// A sha256 hex string generally.
    #[serde(alias = "blob_id")]
    id: String,
    /// Size of the compressed blob file.
    compressed_size: u64,
    /// Size of the uncompressed blob file, or the cache file.
    uncompressed_size: u64,
    /// Chunk size.
    chunk_size: u64,
    /// Number of chunks in blob file.
    /// A helper to distinguish bootstrap with extended blob table or not:
    ///     Bootstrap with extended blob table always has non-zero `chunk_count`
    chunk_count: u64,
    /// Compression algorithm to process the blob.
    compressor: String,
    /// Message digest algorithm to process the blob.
    digester: String,
}

impl DatasetMeta{

    pub fn maxsize() -> Result<String> {

        let json = serde_json::to_string( &json!({
            "max_size:": (RAFS_MAX_CHUNK_SIZE >> 20) * (RAFS_MAX_CHUNKS_PER_BLOB >> 20) as u64,
        }))?;

        Ok(json)
    }
}