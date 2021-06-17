import os

from datetime import datetime

def main():
    print('-----------')
    print(datetime.now())
    print(os.environ['USER_ID'])
    print('-----------')

main()