const md5 = require("md5");
const getClient = require("./minio-client");

const SET_SERVICE_INFO_BUCKET = "set-service-info";
const SET_SERVICE_UPDATES_BUCKET= "SET_SERVICE_UPDATES";
const baseFileType = /\.[^\.]+$/
const metaMetaData = {"Content-Type":"application/json"};


class Client{
  constructor(){
    this.minioClient = getClient();
  }
  
  static getPath(fileName, meta){
    if(meta){
      const metaName = fileName + ".json";
      return `metas-${md5(metaName).substr(0, 5)}-${metaName}`;
    } else {
      const objectPath = `objects-${md5(fileName).substr(0,5)}-${fileName}`;
      return encodeURIComponent(objectPath);
    }
  }
  
  static transformMetaListItem(item){
    const {name, prefix, size, etag, lastModified} = item;
    var [pref, hash, ...rest] = name.split("-");
    var fileName = rest.join("-").replace(baseFileType, "");
    return {
      name:fileName,
      prefix: pref + "-" + hash + "-",
      size,
      etag,
      lastModified
    };
  }
  
  static transformObjectListItem(item){
    const {name, prefix, size, etag, lastModified} = item;
    var [pref, hash, ...rest] = name.split("-");
    var fileName = rest.join("-");
    return {
      name:fileName,
      prefix: pref + "-" + hash + "-",
      size,
      etag,
      lastModified
    };
  }
  
  async getObjectMeta(category, fileName){
    return this.minioClient.getObjectAsync(category, Client.getPath(fileName, true))
      .then((stream) => {
        return new Promise((res, rej) => {
          const buffer = new Buffer([]);
          stream.on('data', function(chunk) {
            buffer = Buffer.concat([buffer, chunk]);
          });
          stream.on('end', function() {
            res(JSON.parse(buffer.toString("utf8")));
          });
          stream.on('error', function(err) {
            rej(err);
          });
        });
      });
  }
  
  async getObjectMetaStream(category, fileName){
    return this.minioClient.getObjectAsync(category, Client.getPath(fileName, true));
  }
  
  async putObjectMeta(category, fileName, document){
    const buffer = Buffer.from(JSON.stringify(document));
    return this.minioClient.putObjectAsync(category, Client.getPath(fileName, true), buffer, buffer.length, metaMetaData); 
  }
  
  async putMetaIfAbsent(category, fileName, buffer, metaData){
    try{
      const obj = await this.getObjectMetaStream(category, fileName);
      if(obj){
        return;
      }
      return this.putObjectMeta(category, fileName, buffer);
    } catch(err){
      return this.putObjectMeta(category, fileName, buffer);
    }
  }
  
  async getObjectStream(){
    return this.minioClient.getObjectAsync(category, Client.getPath(fileName, false));
  }
  
  async getObject(category, fileName, deserialize){
    return this.minioClient.getObjectAsync(category, Client.getPath(fileName, false))
      .then((stream) => {
        return new Promise((res, rej) => {
          var buffer = new Buffer([]);
          stream.on('data', function(chunk) {
            buffer = Buffer.concat([buffer, chunk]);
          });
          stream.on('end', function() {
            res(deserialize(buffer));
          });
          stream.on('error', function(err) {
            rej(err);
          });
        });
      });
  }
  
  async putIfAbsent(category, fileName, buffer, metaData){
    try{
      const obj = await this.getObjectStream(category, fileName);
      if(obj){
        return;
      }
      return this.putObject(category, fileName, buffer, metaData);
    } catch(err){
      return this.putObject(category, fileName, buffer, metaData);
    }
  }
  
  async putStreamIfAbsent(category, fileName, stream, metaData){
    try{
      const obj = await this.getObjectStream(category, fileName);
      if(obj){
        return;
      }
      return this.putObjectStream(category, fileName, stream, metaData);
    } catch(err){
      return this.putObjectStream(category, fileName, stream, metaData);
    }
  }
  
  async putObject(category, fileName, buffer, metaData){
    return this.minioClient.putObjectAsync(category, Client.getPath(fileName, false), buffer, buffer.length, metaData); 
  }
  
  async putObjectStream(category, fileName, stream, metaData){
    return this.minioClient.putObjectAsync(category, Client.getPath(fileName, false), stream, undefined, metaData);  
  }
  
  async getCategories(){
    return this.minioClient.listBucketsAsync().then((buckets) => {
      return buckets.filter((bucket) => {
        return bucket !== SET_SERVICE_INFO_BUCKET && bucket !== SET_SERVICE_UPDATES_BUCKET;
      });
    });
  }
  
  async getCategoryMetaList(category){
    const emitter = this.minioClient.listObjectsV2(category, "metas-");
    return new Promise((res, rej) => {
      const list = [];
      emitter.on("data", (data) => {
        list.push(Client.transformMetaListItem(data));
      });
      emitter.on("error", (err) => {
        rej(err);
      });
      emitter.on("end", () => {
        res(list);
      });
    });
  }
  
  async getCategoryObjectList(category){
    const emitter = this.minioClient.listObjectsV2(category, "objects-");
    return new Promise((res, rej) => {
      const list = [];
      emitter.on("data", (data) => {
        list.push(Client.transformObjectListItem(data));
      });
      emitter.on("error", (err) => {
        rej(err);
      });
      emitter.on("end", () => {
        res(list);
      });
    });
  }
  
  async removeObject(category, fileName){
    return this.minioClient.removeObjectAsync(category, Client.getPath(fileName, false))
  }
  
  async removeObjectMeta(category, fileName){
    return this.minioClient.removeObjectAsync(category, Client.getPath(fileName, true))
  }
  
  async purgeObject(category, fileName){
    return Promise.all([this.removeObject(category, fileName), this.removeObjectMeta(category, fileName)]);
  }
  
  async createCategory(category, region){
    const categories = await this.getObject(SET_SERVICE_INFO_BUCKET, "categories.json", (buffer) => {
      return JSON.parse(buffer.toString("utf8"));
    });

    if(categories.indexOf(category) > -1){
      return;
    }
    
    await this.minioClient.makeBucketAsync(category, region || "us-east-1");
    categories.push(category);
    
    return await this.putObject(SET_SERVICE_INFO_BUCKET, "categories.json", Buffer.from(JSON.stringify(categories)), {"Content-Type": "application/json"});
  }
  
  async removeCategory(category, region){
    const categories = await this.getObject(SET_SERVICE_INFO_BUCKET, "categories.json", (buffer) => {
      return JSON.parse(buffer.toString("utf8"));
    });
    const index = categories.indexOf(category);
    
    if(index === -1){
      return;
    }
    
    categories.splice(index, 1);
    await this.minioClient.removeBucketAsync(category, bucket);
    
    return await this.putObject(SET_SERVICE_INFO_BUCKET, "categories.json", Buffer.from(JSON.stringify(categories)), {"Content-Type": "application/json"});
  }
  
}

module.exports = Client;