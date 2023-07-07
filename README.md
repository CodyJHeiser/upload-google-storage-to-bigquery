```markdown
# Upload Google Storage to BigQuery

This is an npm package that provides functionality to upload files from Google Cloud Storage to BigQuery.

## Installation

You can install the package using npm:

```bash
npm install upload-google-storage-to-bigquery
```

## Usage

```javascript
import { BigQueryUploader } from 'upload-google-storage-to-bigquery';

const uploader = new BigQueryUploader('path/to/keyfile.json');

uploader.uploadToGCS('path/to/file.csv', 'bucket-name')
  .then(() => {
    console.log('File uploaded to Google Cloud Storage successfully.');
  })
  .catch((error) => {
    console.error('Failed to upload file to Google Cloud Storage:', error);
  });

uploader.loadToBigQuery('dataset-id', 'table-id', 'bucket-name', 'path/to/file.csv')
  .then(() => {
    console.log('File loaded to BigQuery successfully.');
  })
  .catch((error) => {
    console.error('Failed to load file to BigQuery:', error);
  });
```

Replace `'path/to/keyfile.json'` with the path to your Google Cloud service account key file. Make sure you have the necessary permissions to access Google Cloud Storage and BigQuery.

## API

### `BigQueryUploader`

The `BigQueryUploader` class provides the following methods:

#### `constructor(keyFilename: string)`

Creates a new instance of the `BigQueryUploader` class with the specified Google Cloud service account key file.

#### `uploadToGCS(filePath: string, bucketName: string): Promise<void>`

Uploads the specified file to the specified Google Cloud Storage bucket.

- `filePath`: The path to the file to upload.
- `bucketName`: The name of the Google Cloud Storage bucket.

#### `loadToBigQuery(datasetId: string, tableId: string, bucketName: string, filePath: string): Promise<void>`

Loads the specified file from Google Cloud Storage to BigQuery.

- `datasetId`: The ID of the BigQuery dataset.
- `tableId`: The ID of the BigQuery table.
- `bucketName`: The name of the Google Cloud Storage bucket.
- `filePath`: The path to the file in Google Cloud Storage.

## Contributing

Contributions are welcome! If you find any issues or have suggestions for improvements, please feel free to open an issue or submit a pull request.

## License

This package is [MIT licensed](LICENSE).
```

Feel free to customize and modify the README file according to your package's specific features and requirements.