from datetime import datetime

def main():
    print('-----------')
    print(datetime.now())
    print(os.environ['USER_ID'])
    print(os.environ['MINIO_ACCESS_KEY'])
    print(os.environ['MINIO_SECRET_KEY'])
    print('-----------')

main()