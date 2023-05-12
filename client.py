"""
docker network ls 
docker network inspect 
docker network connect <containername> 
call qua servicename-port 
set secure = flase 
telnet localhost:port to test  
"""
from minio import Minio 
url = "vutt-minio:9000"
# url = "host.docker.internal:9000"
# url = "vutt-minio:9001"
# url = "127.0.0.1:9003"
client = Minio(url,
               access_key='vutt',
               secret_key='vuttminio', secure=False)


for buc in client.list_buckets():
    print(buc.name)

