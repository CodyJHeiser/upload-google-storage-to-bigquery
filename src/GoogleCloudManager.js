import { BigQuery } from '@google-cloud/bigquery';
import { Storage } from '@google-cloud/storage';
import { extname } from 'path';

/**
 * Represents a manager for interacting with Google Cloud services, including BigQuery and Google Cloud Storage.
 */
class GoogleCloudManager {
    /**
     * Creates a new instance of the GoogleCloudManager class.
     * @param {string} keyFilename The path to the Google Cloud service account key file.
     */
    constructor(keyFilename) {
        this.keyFilename = keyFilename;
        this.bigquery = new BigQuery({ keyFilename: this.keyFilename });
        this.storage = new Storage({ keyFilename: this.keyFilename });
    }

    /**
     * Uploads the specified file to the specified Google Cloud Storage bucket.
     * @param {string} filePath The path to the file to upload.
     * @param {string} bucketName The name of the Google Cloud Storage bucket.
     * @returns {Promise<void>} A Promise that resolves when the upload is successful.
     * @throws {Error} If the file type is invalid. Only .csv and .tsv files are allowed.
     */
    async uploadToGCS(filePath, bucketName) {
        const extension = extname(filePath);
        if (extension !== '.csv' && extension !== '.tsv') {
            throw new Error('Invalid file type. Only .csv and .tsv files are allowed.');
        }

        const bucket = this.storage.bucket(bucketName);

        try {
            await bucket.upload(filePath, {
                gzip: true,
                metadata: {
                    cacheControl: 'public, max-age=31536000',
                },
            });

            console.log(`${filePath} uploaded to ${bucketName}.`);
        } catch (error) {
            console.error(`Failed to upload ${filePath} to ${bucketName}.`, error);
            throw error;
        }
    }

    /**
     * Loads the specified file from Google Cloud Storage to BigQuery.
     * @param {string} datasetId The ID of the BigQuery dataset.
     * @param {string} tableId The ID of the BigQuery table.
     * @param {string} bucketName The name of the Google Cloud Storage bucket.
     * @param {string} filePath The path to the file in Google Cloud Storage.
     * @returns {Promise<void>} A Promise that resolves when the load is successful.
     */
    async loadToBigQuery(datasetId, tableId, bucketName, filePath) {
        await this.uploadToGCS(filePath, bucketName);

        const extension = extname(filePath);
        let sourceFormat = 'CSV';
        let fieldDelimiter = ',';
        if (extension === '.tsv') {
            sourceFormat = 'CSV';
            fieldDelimiter = '\t';
        }

        const metadata = {
            sourceFormat: sourceFormat,
            skipLeadingRows: 1,
            autodetect: true,
            fieldDelimiter: fieldDelimiter,
        };

        const dataset = this.bigquery.dataset(datasetId);
        const table = dataset.table(tableId);
        const exists = await table.exists();
        if (!exists[0]) {
            await table.create();
            console.log(`Table ${tableId} created.`);
        }

        const fileName = filePath.split('/').pop();

        const [job] = await table
            .load(this.storage.bucket(bucketName).file(fileName), metadata);

        console.log(`Job ${job.id} completed.`);
    }

    /**
     * Runs the specified SQL query in BigQuery.
     * @param {string} sql The SQL query to run.
     * @returns {Promise<any>} A Promise that resolves with the query response or rejects with an error.
     */
    async runQuery(sql) {
        try {
            const sqlResponse = await this.bigquery.query(sql);
            return sqlResponse;
        } catch (err) {
            return err;
        }
    }
}

export default GoogleCloudManager;