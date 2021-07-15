
import sys
import os
import boto3
import time

# -*- coding: utf-8 -*-
# ec2 경로내 파일 유무 체크하는 함수
def ec2_check(dir):
    
    # ec2 경로에 데이터 파일 리스트 확인
    file_lists = os.listdir(dir)

    # 경로에 저장된 파일 리스트 출력
    file_dir_list = []
    for file in file_lists:
        file_dir = dir + '/' + file
        file_dir_list.append(file_dir)

    print(f'파일 경로 리스트 : {file_dir_list}')
    print('')
    return file_dir_list


# s3 버캣 리스트 출력 후 원하는 버캣 선택하는 함수 - 없을 경우에 버캣 생성
def search_bucket_new(wanna_name):

    # 원하는 버킷 생성하기
    client.create_bucket(Bucket=wanna_name, CreateBucketConfiguration={'LocationConstraint': 'ap-northeast-2'})  # 새롭게 버켓을 생성하고
    print("새로운 버킷을 생성했으며, 해당 버킷으로 데이터를 import : ", wanna_name)
    print('')
    return wanna_name  # 해당 버켓을 출력


# s3 버캣 리스트 출력 후 원하는 버캣 선택하는 함수 - 없을 경우에 버캣 생성
def search_bucket_old(client, bucket_list):

    # 원하는 버킷 선택하기
    while True:
        wanna_bucket = str(input("데이터를 넣고자하는 bucket 을 선택해주세요. : "))

        if wanna_bucket in bucket_list:  # 만약 입력한 버켓이 리스트 내에 존재할 경우
            print("해당 버킷으로 데이터를 import : ", wanna_bucket)
            print('')
            return wanna_bucket  # 해당 버켓을 출력
            break
        else:
            print('다시 입력해 주세요')  # 잘못 입력하거나 오타가 날 경우 위 과정 재시도
            print('')
            continue


# 새롭게 파일을 업로드 하는 함수
def ec2_s3(file_lists, bucket_name, client):

    # 선택한 버켓 내 경로 생성 여부 판별
    print("1 : 사용자 지정경로 / 2 : 날짜 경로(당일날짜 자동 생성) / 3 : 생성하지 않음(버킷에 그대로 업로드) ")
    choice_num = int(input("번호를 입력해주세요 : "))

    new_file = []
    if choice_num == 1: # 사용자 지정 경로
        user_dir = str(input("원하는 경로 이름을 입력해주세요 : "))
        for file in file_lists:
            # 경로에서 s3 에 업로드할 파일 이름 생성
            hehe = file.split('/')[-1]
            # 파일 s3 에 업로드
            client.put_object(Bucket=bucket_name, Body=file, Key= user_dir + '/' + hehe)  # 첫번째 매개변수 : S3 버킷 이름, #두번째 매개변수 : 로컬에서 올릴 파일이름,  # 세번째 매개변수 : 버킷에 저장될 파일 이름
            new_file.append(hehe)

    elif choice_num == 2: # 날짜 경로 (당일 날짜 자동 생성)
        # 파일 업로드
        for file in file_lists:
            # 경로에서 s3 에 업로드할 파일 이름 생성
            hehe = file.split('/')[-1]
            # 파일 s3 에 업로드 (업로드 날자별 파티셔닝 생성하여 업로드)
            client.put_object(Bucket=bucket_name, Body=file, Key=(time.strftime('%Y-%m-%d', time.localtime(time.time()))) + '/' + hehe)  # 첫번째 매개변수 : S3 버킷 이름, #두번째 매개변수 : 로컬에서 올릴 파일이름,  # 세번째 매개변수 : 버킷에 저장될 파일 이름
            new_file.append(hehe)

    elif choice_num == 3: # 생성하지 않음
        #파일 업로드
        for file in file_lists:
            # 경로에서 s3 에 업로드할 파일 이름 생성
            hehe = file.split('/')[-1]
            #파일 s3 에 업로드
            client.put_object(Bucket=bucket_name,Body=file, Key=hehe) # 첫번째 매개변수 : S3 버킷 이름, #두번째 매개변수 : 로컬에서 올릴 파일이름,  # 세번째 매개변수 : 버킷에 저장될 파일 이름
            new_file.append(hehe)

    # 최초로 업데이트 된 파일
    print("최초 업데이트 파일 : ", new_file)
    print('')


# 기존 경로에 존재하는 파일중 일부를 업데이트 및 새로운 파일 추가
def ec2_s3_del_up(file_lists, bucket_name, client):

    # s3 인증 및 파일 리스트 가져오기위한 절차
    paginator = client.get_paginator('list_objects_v2')
    response_iterator = paginator.paginate(Bucket=bucket_name)

    # s3 경로 파일 저장된 파일 리스트 추출 (list comprehension 3중 for 문)
    s3_lists = [s3_lists for page in response_iterator for s3_lists in page['Contents'] for list in range(len(s3_lists))]

    s3_file_list = []
    for i in range(len(s3_lists)):
        s3_file_list.append(s3_lists[i]['Key'])

    # 중복된 리스트 부분을 제거한 후 다시 리스트로 만들기
    s3_file_list = list(set(s3_file_list)) # 중복을 제거하기 위해 set() 함수 사용하여 set으로 만들고 난 후 list화
    print(s3_file_list)
    print('')

    update_file = []
    stay_file = []

    # 리스트를 비교하여 s3 파일 업데이트 및 추가하기
    for file in file_lists:
        # 절대경로에서 파일이름 부분만 가져오기
        file = file.split('/')[-1]
        # 외부에서 넣는 파일과 s3내 존재하는 파일이 동일할 경우
        if file in s3_file_list:  # 같은 파일이 리스트 내 존재할 경우에
            # 해당 파일(s3_lst)을 삭제하고
            client.delete_object(Bucket=bucket_name, Key=file) # 첫번째 매개변수 : S3 버킷 이름, #두번째 매개변수 : 버킷에서 삭제할 파일 이름
            # 새로운 파일(list) 를 다시 업로드
            client.put_object(Bucket=bucket_name, Body=file, Key=file)  # 첫번째 매개변수 : S3 버킷 이름, #두번째 매개변수 : 로컬에서 올릴 파일이름,  # 세번째 매개변수 : 버킷에 저장될 파일 이름
            update_file.append(file)

        else:  # 같은 파일이 존재하지 않을 경우에 그냥 업로드
            client.put_object(Bucket=bucket_name, Body=file, Key=file)  # 첫번째 매개변수 : S3 버킷 이름, #두번째 매개변수 : 로컬에서 올릴 파일이름,  # 세번째 매개변수 : 버킷에 저장될 파일 이름
            stay_file.append(file)

    # 각각 리스트 체크
    print("기존에서 업데이트된 파일 : ",  update_file)
    print("새로 추가된 파일 : ", stay_file)
    print('')


if __name__ == "__main__":

    # requirements 입력
    check_dir = input('파일 경로를 입력해주세요 : ') #/home/jinchoe/jin_test  # 파일 체크할 ec2 경로 입력
    file_list = ec2_check(check_dir)
    #버킷 존재 유무 확인

    # s3 client 생성하기
    client = boto3.client('s3',
                aws_access_key_id='',
                aws_secret_access_key=''
             )

    # 현재 있는 버킷 리스트 정보 가져오기
    bucket_lists = client.list_buckets()

    # bucket_lists에서 bucket 이름만 가져와 저장
    bucket_list = [bucket_name['Name'] for bucket_name in bucket_lists['Buckets']]
    print(bucket_list, "이 중에서 원하는 버켓이 존재 하나요? - 숫자로 입력해주세요")
    yes_or_not = int(input("Yes - 1 / No - 2 : "))

    while True:
        #원하는 버켓이 존재할 경우 - 파일 업데이트
        if yes_or_not == 1:
            bucket_name = search_bucket_old(client, bucket_list)
            ec2_s3_del_up(file_list, bucket_name, client)
            break
        # 원하는 버킷이 존재하지 않을 경우 - 버킷 생성후 파일 업데이트
        elif yes_or_not == 2:
            wanna_name = str(input('생성할 버킷 이름을 입력해주세요 : '))
            bucket_name = search_bucket_new(wanna_name)
            ec2_s3(file_list, bucket_name, client)
            break
        else:
            print('잘못입력했습니다. 다시 입력해주세요')

