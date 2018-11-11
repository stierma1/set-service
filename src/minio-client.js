const Minio = require('minio');
const {promisify} = require("util");

var minioClient = null;
const promisifyMethods = [
  "getObject",
  "putObject",
  "makeBucket",
  "removeBucket",
  "listBuckets",
  "listObjects",
  "listObjectsV2",
  "statObject",
  "removeObject",
  "removeObjects",
  "getBucketPolicy",
  "setBucketPolicy"
];

module.exports = function getClient(properties) {
  if(minioClient){
    return minioClient;
  }

  const {endPoint, port, useSSL, accessKey, secretKey} = (properties || {});
  
  minioClient = new Minio.Client({endPoint, port, useSSL, accessKey, secretKey});
  promisifyMethods.map((method) => {
    minioClient[method + "Async"] = promisify(minioClient[method]);
  })

  return minioClient;
}