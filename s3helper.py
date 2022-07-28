import boto3

class S3:

    def __init__(self, access_key_id: str, secret_access_key: str, region: str):
        self.client = boto3.client('s3', aws_access_key_id = access_key_id, aws_secret_access_key = secret_access_key, region_name = region)

        self.bucket_information = self.client.list_buckets()
        self.buckets = self.bucket_information['Buckets']
        self.owner = self.bucket_information['Owner']['DisplayName']
        self.location = {'LocationConstraint': region}

    def get_object(self, bucket_name: str, filename: str):

        obj = self.client.get_object(
            Bucket = bucket_name,
            Key = filename
        )

        return obj

    def download(self, bucket_name: str, from_filename: str, to_filename: str):
        """Downloads a file from the s3 bucket and saves it to a filename of your choice

        Args:
            bucket_name (str): Name of the bucket to get the data from
            from_filename (str): Name of the object in the bucket
            to_filename (str): Filename to save this to
        """

        self.client.download_file(
            Bucket = bucket_name,
            Key = from_filename,
            Filename = to_filename
        )

    def upload(self, bucket_name: str, from_filename: str, to_filename: str):
        """Uploads a file to a bucket with a certain name

        Args:
            bucket_name (str): S3 bucket that you would like to upload this data to
            from_filename (str): Filename of the file that you would like to send this to
            to_filename (str): Filename for the new file in the bucket
        """

        self.client.upload_file(
            Bucket = bucket_name,
            Filename = from_filename,
            Key = to_filename
        )

    def create_new_bucket(self, new_bucket_name: str):
        """Creates a new s3 bucket in the default location of this helper

        Args:
            new_bucket_name (str): _description_
        """

        self.client.create_bucket(
            Bucket = new_bucket_name
        )

    def get_key_list_from_bucket(self, bucket_name: str):
        """Returns a list of the filenames in a bucket

        Args:
            bucket_name (str): Bucket name that you want to search
        """

        bucket_objects = self._client.list_objects_v2(
            Bucket = bucket_name
        )

        key_names = []
        for key in bucket_objects['Contents']:
            key_names.append(key['Key'])

        return key_names

    def get_keys_of_file_type_from_bucket(self, bucket_name: str, filetype: str) -> list:
        """Returns a list of filenames in a bucket that have a certain file extension

        Args:
            bucket_name (str): Bucket name that you want to search
            filetype (str): File extension that you want to look for. For example
            if you want to look for .csv files, you can enter ".csv".

        Returns:
            list: List of the filenames that fit this search
        """

        file_list_from_bucket = self.get_key_list_from_bucket(bucket_name=bucket_name)

        new_file_list = [f for f in file_list_from_bucket if filetype in f]
        return new_file_list