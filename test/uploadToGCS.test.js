import GoogleCloudManager from '../src/GoogleCloudManager.js';
import { Storage } from '@google-cloud/storage';
import { BigQuery } from '@google-cloud/bigquery';

jest.mock('@google-cloud/storage');
jest.mock('@google-cloud/bigquery');

describe('GoogleCloudManager', () => {
    const mockUpload = jest.fn();
    const mockLoad = jest.fn();
    const mockExists = jest.fn();

    beforeAll(() => {
        Storage.prototype.bucket = jest.fn().mockReturnValue({ upload: mockUpload });
        BigQuery.prototype.dataset = jest.fn().mockReturnValue({
            table: jest.fn().mockReturnValue({ load: mockLoad, exists: mockExists })
        });
    });

    beforeEach(() => {
        jest.clearAllMocks();
    });

    it('should upload a file to GCS', async () => {
        const manager = new GoogleCloudManager("auth/google-cloud-key-file.json");
        await manager.uploadToGCS('test/file.csv', 'ua-uploads');

        expect(mockUpload).toHaveBeenCalled();
    });

    it('should load a file to BigQuery', async () => {
        mockExists.mockResolvedValueOnce([true]);  // Assuming the table already exists
        const manager = new GoogleCloudManager("auth/google-cloud-key-file.json");
        await manager.loadToBigQuery('my-bigquery-dataset', 'my-bigquery-table', 'ua-uploads', 'test/file.csv');

        expect(mockLoad).toHaveBeenCalled();
    });
});
