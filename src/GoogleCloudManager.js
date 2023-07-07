import { BigQuery } from '@google-cloud/bigquery';
import { Storage } from '@google-cloud/storage';
import { extname } from 'path';

class GoogleCloudManager {
    constructor(keyFilename) {
        this.keyFilename = keyFilename;
        this.bigquery = new BigQuery({ keyFilename: this.keyFilename });
        this.storage = new Storage({ keyFilename: this.keyFilename });
    }

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
}

export default GoogleCloudManager;