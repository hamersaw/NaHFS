use hdfs_comm::rpc::{Server, Protocol};
use hdfs_protos::hadoop::hdfs::{GetFileInfoResponseProto, GetFileInfoRequestProto};
use prost::Message;

use crate::file::FileStore;

use std::sync::{Arc, RwLock};

pub struct ClientProtocol {
    file_store: Arc<RwLock<FileStore>>,
}

impl ClientProtocol {
    pub fn new(file_store: Arc<RwLock<FileStore>>) -> ClientProtocol {
        ClientProtocol {
            file_store: file_store,
        }
    }

    fn get_file_info(&self, req_buf: &[u8], resp_buf: &mut Vec<u8>) {
        let request = GetFileInfoRequestProto
            ::decode_length_delimited(req_buf);

        // TODO - get file

        let response = GetFileInfoResponseProto::default();
        response.encode_length_delimited(resp_buf);
    }
}

impl Protocol for ClientProtocol {
    fn process(&self, method: &str, req_buf: &[u8],
            resp_buf: &mut Vec<u8>) {
        match method {
            "getFileInfo" => self.get_file_info(req_buf, resp_buf),
            _ => println!("unimplemented method '{}'", method),
        }
    }
}
