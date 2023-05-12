from minio import Minio
import pandas as pd
import io, pickle, os

__author__ = "Rahul Rawat"

class Minio_db():

    def __init__(self):

        # Initialize minioClient with an endpoint and access/secret keys.
        try:

            self.minioClient = Minio("hostPath", access_key="db_user", secret_key="db_key", secure=False)

        except Exception as ex:
            print("Not able to connect minio / {}".format(ex))

    def get_data(self, bucket_name, object_name, type="pkl"):
        """
        fetch object from bucket based on type
        :param bucket_name: Container name in Minio : str
        :param object_name: name of minio object : str
        :param type: type of object ("flatten", "pkl")
        :return: dataframe : Boolean
        """
        try:
            """checking the bucket exist or not"""
            bucket = self.minioClient.bucket_exists(bucket_name)
            if bucket:
                if type == "flatten":
                    get_data = self.minioClient.get_object(bucket_name, object_name).read().decode('utf-8')
                    data = pd.read_csv(io.StringIO(get_data))
                    df = pd.DataFrame(data)
                    print("Dataset loaded successfully.")
                    return df
                elif type == "pkl":
                    # fetching the trained model from the minio database based on the selection..
                    model_data = self.minioClient.get_object(bucket_name, object_name)
                    data = pickle.load(model_data)
                    print("Model loaded sucessfully..")
                    return data
                else:
                    print(str="Dataset can't be loaded because Bucket does not exist..")
                    return None
            else:
                print("Bucket does not exist")
        except Exception as ex:
            print("Not able to get data from minio / {}".format(ex))
    def insert_data(self, data, bucket_name, object_name, toCreateNewBucket=False):
        """
                insert object into bucket based on type
                :param bucket_name: Container name in Minio : str
                :param object_name: name of minio object : str
                :param toCreateNewBucket: option to create new bucket ("default value: False")
                :return: status : True or False
                """
        try:
            bucket = self.minioClient.bucket_exists(bucket_name)
            isSuccess = False
            if bucket:
                df = pickle.dumps(data)
                self.minioClient.put_object(bucket_name, object_name, data=io.BytesIO(df), length=len(df))
                print("Dataset/Model has been sucessfully saved in {}".format(bucket_name))
                isSuccess = True
            elif toCreateNewBucket:
                self.minioClient.make_bucket(bucket_name)
                print("Bucket created sucessfully for saving a model..")
                self.insert_data(data, bucket_name, object_name)
                isSuccess = True
        except Exception as ex:
            print("Not able to insert data into minio/ {}".format(ex))
        return isSuccess
    def delete_model(self, bucket_name, object_name):
        """
                        delete object from bucket based on type
                        :param bucket_name: Container name in Minio : str
                        :param object_name: name of minio object : str
                        :return: status : True or False
                        """
        try:

            bucket = self.minioClient.bucket_exists(bucket_name)
            isSuccess = False
            if bucket:
                # deleting/removing the previously trained model from the minio database based on the selection...
                self.minioClient.remove_object(bucket_name, object_name)
                print("Object deleted sucessfully..")
                isSuccess = True

            else:
                print("Object can't be deleted beacuase Bucket is not available..")

        except Exception as ex:
            print("Object can not be deleted/ {}".format(ex))

        return isSuccess