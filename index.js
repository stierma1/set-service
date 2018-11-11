const createClient = require("./src/minio-client");
const Client = require("./src/client");
var client = null;
module.exports = function getSetServiceClient(props){
  if(client){
    return client;
  }
  const {MINIO_HOST, MINIO_PORT, MINIO_SSL, MINIO_ACCESS_KEY, MINIO_SECRET_KEY} = process.env;
  const properties = {endPoint:MINIO_HOST, port:MINIO_PORT, useSSL:MINIO_SSL === "true", accessKey:MINIO_ACCESS_KEY, secretKey:MINIO_SECRET_KEY};
  createClient(props || properties);
  client = new Client();
  return client;
}
