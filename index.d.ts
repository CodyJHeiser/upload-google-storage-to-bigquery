/**
 * Represents a BigQueryUploader instance for uploading files from Google Cloud Storage to BigQuery.
 */
declare module 'upload-google-storage-to-bigquery' {
  /**
   * BigQueryUploader class.
   */
  export class BigQueryUploader {
    /**
     * Creates a new instance of the BigQueryUploader class with the specified Google Cloud service account key file.
     * @param {string} keyFilename The path to the Google Cloud service account key file.
     */
    constructor(keyFilename: string);

    /**
     * Uploads the specified file to the specified Google Cloud Storage bucket.
     * @param {string} filePath The path to the file to upload.
     * @param {string} bucketName The name of the Google Cloud Storage bucket.
     * @returns {Promise<void>} A Promise that resolves when the upload is successful.
     */
    uploadToGCS(filePath: string, bucketName: string): Promise<void>;

    /**
     * Loads the specified file into Google Cloud Storage via uploadToGCS method, then transfers data from GCS to BigQuery.
     * @param {string} datasetId The ID of the BigQuery dataset.
     * @param {string} tableId The ID of the BigQuery table.
     * @param {string} bucketName The name of the Google Cloud Storage bucket.
     * @param {string} filePath The path to the file in Google Cloud Storage.
     * @returns {Promise<void>} A Promise that resolves when the load is successful.
     */
    loadToBigQuery(datasetId: string, tableId: string, bucketName: string, filePath: string): Promise<void>;

    /**
     * Runs the specified SQL query in BigQuery.
     * @param {string} sql The SQL query to run.
     * @returns {Promise<any>} A Promise that resolves with the query response or rejects with an error.
     */
    runQuery(sql: string): Promise<any>;
  }
}