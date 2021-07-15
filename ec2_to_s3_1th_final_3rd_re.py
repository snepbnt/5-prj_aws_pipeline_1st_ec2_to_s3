
import sys
import os
import boto3
import time
from enum import Enum

# -*- coding: utf-8 -*-

# s3 client 생성하기
client = boto3.client('s3',
            aws_access_key_id='',
            aws_secret_access_key=''
         )

# 현재 있는 버킷 리스트 정보 가져오기
bucket_lists = client.list_buckets()

# bucket_lists에서 bucket 이름만 가져와 저장
bucket_list = [bucket_name['Name'] for bucket_name in bucket_lists['Buckets']]
bucket_list = Enum('bucket_list',bucket_list)

# 메뉴 선택하는 함수
def select_menu() -> bucket_list:
    """메뉴 선택"""
    s = [f'({m.value}){m.name}' for m in bucket_list]
    while True:
        print(*s, sep = '  ', end='')
        n = int(input(' : '))
        if 1 <= n <= len(bucket_list):
            return bucket_list(n)


# ec2 경로내 파일 유무 체크하는 함수
def ec2_check(dir):

    file_list = []
    # ec2 경로에 데이터 파일 리스트 확인
    path2 = os.path.dirname(dir)
    for path, dirs, files, in os.walk(path2):
        # 경로 내 파일 탐색 후 리스트 저장
        for file in files:
            file_path = os.path.join(path, file)
            file_list.append(file_path.replace('\\','/'))

    print(f'파일 경로 리스트 : {file_list}')
    return file_list


# 버킷내 DB 리스트 생성

# 새롭게 파일을 업로드 하는 함수
def ec2_s3_cre(file_lists, bucket_name, client, directions):
    
    # 1. S3 버킷 내 파일 리스트 만들기
    # s3 인증 및 파일 리스트 가져오기위한 절차
    paginator = client.get_paginator('list_objects_v2')
    response_iterator = paginator.paginate(Bucket=bucket_name)

    # s3 경로 파일 저장된 파일 리스트 추출 (list comprehension 3중 for 문)
    s3_lists = [s3_lists for page in response_iterator for s3_lists in page['Contents'] for list in
                range(len(s3_lists))]

    s3_file_list = []
    for i in range(len(s3_lists)):
        s3_file_list.append(s3_lists[i]['Key'])

    # 중복된 리스트 부분을 제거한 후 다시 리스트로 만들기
    s3_file_list = sorted(list(set(s3_file_list)))  # 중복을 제거하기 위해 set() 함수 사용하여 set으로 만들고 난 후 list화
    s3_file_list = [s3_file_list[i] for i in range(len(s3_file_list)) if s3_file_list[i].find('.csv') != -1]
    print('')

    #2. 버킷 내 DB 리스트 생성
    paginator = client.get_paginator('list_objects_v2')
    response_iterator = paginator.paginate(Bucket=bucket_name)

    # s3 경로 파일 저장된 파일 리스트 추출 (list comprehension 3중 for 문)
    s3_lists = [s3_lists for page in response_iterator for s3_lists in page['Contents'] for list in range(len(s3_lists))]

    total_dir = []
    dir_1st = []
    for i in range(len(s3_lists)):
        total_dir.append(s3_lists[i]['Key'].split('/'))
        for i in range(len(total_dir)):
            dir_1st.append(total_dir[i][0])

    dir_1st = sorted(list(set(dir_1st)))
    print(dir_1st)

    # 선택한 버켓 내 새로운 경로 생성
    print("1 : EC2 하부경로 자동 생성 / 2 : 날짜 경로(당일날짜 자동 생성) ")
    choice_num = int(input("번호를 입력해주세요 : "))

    new_file = []
    if choice_num == 1: # EC2 하부경로 자동 생성
        # 리스트 메뉴를 만들기
        select_Menu = Enum('select_Menu', dir_1st)
        s = [f'({m.value}){m.name}' for m in select_Menu]
        print(*s, sep='  ', end='')
        n = int(input(' DB 번호를 입력하세요 : '))

        # 빈 리스트 생성
        update_file = []
        stay_file = []

        if n != 0: # DB 번호 선택 후
            dir_1st = select_Menu(n).name

            # EC2에 존재하는 파일의 바로 앞 경로를 넣고 새로운 경로 생성
            for file in file_lists:
                print(file)
                # 경로에서 s3 에 업로드할 파일 이름 생성
                hehe = file.split('/')[-1]
                dir_name = file.split('/')[-2] #파일앞 경로 가져오기
                file = dir_1st+'/'+dir_name + '/' + hehe # 파일 존재유무 파악을 위한 파일 생성

                # 외부에서 넣는 파일과 s3내 존재하는 파일이 동일할 경우
                if file in s3_file_list:  # 같은 파일이 리스트 내 존재할 경우에
                    # 해당 파일(s3_lst)을 삭제하고
                    client.delete_object(Bucket=bucket_name,Key=(dir_1st + '/' + dir_name) + '/' + hehe)  # 첫번째 매개변수 : S3 버킷 이름, #두번째 매개변수 : 버킷에서 삭제할 파일 이름
                    # 파일 s3 에 업로드
                    client.put_object(Bucket=bucket_name, Body=file, Key= (dir_1st + '/' + dir_name) + '/' + hehe)  # 첫번째 매개변수 : S3 버킷 이름, #두번째 매개변수 : 로컬에서 올릴 파일이름,  # 세번째 매개변수 : 버킷에 저장될 파일 이름
                    update_file.append(file)
                else:  # 같은 파일이 존재하지 않을 경우에 그냥 업로드
                    client.put_object(Bucket=bucket_name, Body=file, Key= (dir_1st + '/' + dir_name) + '/' + hehe)  # 첫번째 매개변수 : S3 버킷 이름, #두번째 매개변수 : 로컬에서 올릴 파일이름,  # 세번째 매개변수 : 버킷에 저장될 파일 이름
                    stay_file.append(file)

            # 각각 리스트 체크
            print("업데이트된 파일 : ", update_file)
            print("추가된 파일 : ", stay_file)
            print('')


    elif choice_num == 2: # 날짜 경로 (당일 날짜 자동 생성)
        # 리스트 메뉴를 만들기
        select_Menu = Enum('select_Menu', dir_1st)
        s = [f'({m.value}){m.name}' for m in select_Menu]
        print(*s, sep='  ', end='')
        n = int(input(' DB 번호를 입력하세요 : '))

        # 빈 리스트 생성
        update_file = []
        stay_file = []

        if n != 0: # 원하는 경로가 있을 경우
            dir_1st = select_Menu(n).name

            # 파일 업로드
            for file in file_lists:
                # 경로에서 s3 에 업로드할 파일 이름 생성
                hehe = file.split('/')[-1]
                dir_name = file.split('/')[-2] #파일앞 경로 가져오기
                file = dir_1st + '/' + dir_name + '/' + time.strftime('%Y-%m-%d', time.localtime(time.time())) + '/' + hehe  # 파일 존재유무 파악을 위한 파일 생성

                # 외부에서 넣는 파일과 s3내 존재하는 파일이 동일할 경우
                if file in s3_file_list:  # 같은 파일이 리스트 내 존재할 경우에
                    # 해당 파일(s3_lst)을 삭제하고
                    client.delete_object(Bucket=bucket_name, Key=(dir_1st + '/' + dir_name + '/' + time.strftime('%Y-%m-%d', time.localtime(time.time()))) + '/' + hehe)  # 첫번째 매개변수 : S3 버킷 이름, #두번째 매개변수 : 버킷에서 삭제할 파일 이름
                    # 파일 s3 에 업로드
                    client.put_object(Bucket=bucket_name, Body=file, Key=(dir_1st + '/' + dir_name + '/' + time.strftime('%Y-%m-%d', time.localtime(time.time()))) + '/' + hehe)  # 첫번째 매개변수 : S3 버킷 이름, #두번째 매개변수 : 로컬에서 올릴 파일이름,  # 세번째 매개변수 : 버킷에 저장될 파일 이름
                    update_file.append(file)
                else:  # 같은 파일이 존재하지 않을 경우에 그냥 업로드
                    client.put_object(Bucket=bucket_name, Body=file, Key=(dir_1st + '/' + dir_name + '/' + time.strftime('%Y-%m-%d', time.localtime(time.time()))) + '/' + hehe)  # 첫번째 매개변수 : S3 버킷 이름, #두번째 매개변수 : 로컬에서 올릴 파일이름,  # 세번째 매개변수 : 버킷에 저장될 파일 이름
                    stay_file.append(file)

            # 각각 리스트 체크
            print("업데이트된 파일 : ", update_file)
            print("추가된 파일 : ", stay_file)
            print('')


if __name__ == "__main__":

    # requirements 입력
    check_dir = input('파일 경로를 입력해주세요 : ') #/home/jinchoe/jin_test  # 파일 체크할 ec2 경로 입력  ''
    file_list = ec2_check(check_dir)

    # 버킷 선택
    print("업로드할 버킷을 선택해주세요")
    selects = select_menu()
    selects_bucket = selects.name
    print(selects_bucket)

    # 파일 업로드
    ec2_s3_cre(file_list, selects_bucket, client, check_dir)
    print("작업을 완료했습니다.")





