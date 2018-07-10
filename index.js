const Jimp = require("jimp");
const csv = require('fast-csv');
const AWS = require('aws-sdk');
const fs = require('fs');
const endOfLine = require('os').EOL;

const prefixS3origin = 'img/1200x900/';
const prefixS3dest = 'img/blured/';

const { AWS_ACCESSKEYID, AWS_SECRETACCESSKEY } = process.env;

// ventura credentials
AWS.config.update(
  { 
    accessKeyId: AWS_ACCESSKEYID,
    secretAccessKey: AWS_SECRETACCESSKEY,
  }
);

var S3 = new AWS.S3();

function processBuffer(path, mime, cb) {
  return Jimp.read(path)
    .then(function(img) {
      return img
        .resize(50, Jimp.AUTO)
        .blur(10)
        .quality(90)
        .getBuffer(mime, cb)
    })
    .catch(function(e) {
      console.log('error proccesign', e);
    });
}


function readData(csvFile) {
  const Data = [];
  return new Promise(function(done) {
    csv
      .fromPath(csvFile)
      .on('data', function(data) {
        Data.push(data[0])
      })
      .on('end', function() {
        done(Data);
      })
  });

}

function findS3file(bucket, name) {
  const Key = prefixS3origin + name;
  const params = {
    Bucket: bucket,
    Key: Key,
  };
  return S3.getObject(params).promise()
}

function putS3file(bucket, name, body, original) {
  const Key = prefixS3dest + name;
  var params = {
    Body: body,
    Bucket: bucket,
    Key: Key,
    ContentType: original.ContentType,
    CacheControl: original.CacheControl,
    ACL: 'public-read'
  };
  return S3.putObject(params).promise();
}

async function start(csvFile, bucket) {
  //const notFound = [];
  //const found = [];
  const res = await readData(csvFile);
  console.log('Entries: ' + res.length);

  for(let i = 0; i < res.length; i++) {
    const fileName =  res[i];
    // const fileName =  '5b32da8aca63c.jpg';
    let s3File;
    try { 
      s3File = await findS3file(bucket, fileName);
    } catch(e) {
      console.log(i + ' .. ' + fileName)
      fs.appendFileSync('notFound.csv', '"'+ fileName +'"' + endOfLine)
    }

    if (!s3File) continue;
    const { Body, ContentType } = s3File;

    await processBuffer(Body, ContentType, async (st, bufferP) => {
      const toSaveFileName = fileName;
      try {
        await putS3file(bucket, toSaveFileName, bufferP, s3File)
        // save succes files in other file
        console.log(i + ' saved ', toSaveFileName);
        fs.appendFileSync('success.csv', '"'+ toSaveFileName +'"' + endOfLine)
      } catch(e) {
        fs.appendFileSync('errorSaving.csv', '"'+ toSaveFileName +'"' + endOfLine)
        console.log('Error', fileName, e);
      }
    });

    // if (i === 1000) break; // limit first file
    // if (i === 0) break; // limit first file
  }
}

(async () => {
  await start('./query_result.csv', 'vi.images');
})()

