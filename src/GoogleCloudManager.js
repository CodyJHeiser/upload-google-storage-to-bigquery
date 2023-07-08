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

        let bucket = this.storage.bucket(bucketName);

        try {
            // Check if the bucket exists
            const [buckets] = await this.storage.getBuckets();
            const bucketExists = buckets.some(b => b.name === bucketName);

            // If not, create it
            if (!bucketExists) {
                [bucket] = await this.storage.createBucket(bucketName);
                console.log(`Bucket ${bucketName} created.`);
            }
        } catch (error) {
            console.warn(`Failed to check or create bucket ${bucketName}. Will attempt to upload file.`);
        }

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

        const loadMetadata = {
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

        // Load data into a temporary table
        const tempTableId = tableId + "_temp";
        const tempTable = dataset.table(tempTableId);
        const tempExists = await tempTable.exists();
        if (!tempExists[0]) {
            await tempTable.create();
        }

        const [job] = await tempTable
            .load(this.storage.bucket(bucketName).file(fileName), loadMetadata);

        console.log(`Temp table job ${job.id} completed.`);

        // Retrieve the schema
        const [tableMetadata] = await tempTable.getMetadata();
        const schema = tableMetadata.schema.fields.map(field => field.name);

        // Build the ON clause
        const onClause = schema.map(col => `T.${col} = S.${col}`).join(' AND ');

        // Merge data from temporary table to main table
        const query = `MERGE ${datasetId}.${tableId} T
                   USING ${datasetId}.${tempTableId} S
                   ON ${onClause}
                   WHEN NOT MATCHED THEN
                       INSERT ROW`;
        const options = {
            query: query,
            location: 'US',
        };
        const [job2] = await this.bigquery.createQueryJob(options);
        const [rows] = await job2.getQueryResults();

        console.log(`Merge job ${job2.id} completed.`);

        // Delete temporary table
        await tempTable.delete();
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