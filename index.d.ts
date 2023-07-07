declare module 'upload-google-storage-to-bigquery' {
  export class BigQueryUploader {
    constructor(keyFilename: string);
    uploadToGCS(filePath: string, bucketName: string): Promise<void>;
    loadToBigQuery(datasetId: string, tableId: string, bucketName: string, filePath: string): Promise<void>;
  }
}
