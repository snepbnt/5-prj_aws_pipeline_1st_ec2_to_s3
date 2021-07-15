
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

    return file_dir_list



# 경로 내 파일 s3 넣는 함수 lists: 존재하는 파일 리스트, dir: 파일이 위치한 경로, bname : 파일 담을 버켓 이름
def ec2_s3(lists, bucket_name):

    # s3 client 생성하기
    client = boto3.client('s3',
            aws_access_key_id='',
            aws_secret_access_key=''
    )
    new_file = []
    #파일 업로드
    for file in lists:
        # 경로에서 s3 에 업로드할 파일 이름 생성
        print(file)
        hehe = file.split('/')[-1]

        #파일 s3 에 업로드
        client.put_object(Bucket=bucket_name,Body=file, Key=hehe) # 첫번째 매개변수 : S3 버킷 이름, #두번째 매개변수 : 로컬에서 올릴 파일이름,  # 세번째 매개변수 : 버킷에 저장될 파일 이름

        new_file.append(hehe)

    # 최초로 업데이트 된 파일
    print("최초 업데이트 파일 : ", new_file)


# 경로에 파일이 존재할 경우 모두 삭제
def ec2_s3_del_up(lists, bucket_name):

    # s3 인증 및 파일 리스트 가져오기위한 절차
    client = boto3.client('s3',
                        aws_access_key_id='',
                        aws_secret_access_key=''
             )
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

    update_file = []
    stay_file = []

    # 리스트를 비교하여 s3 파일 업데이트 및 추가하기
    for file in lists:
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


if __name__ == "__main__":

    # boto3 자격 증명
    access_key = ''

    # requirements 입력
    check_dir = input('파일 경로를 입력해주세요 : ') #/home/jinchoe/jin_test  # 파일 체크할 ec2 경로 입력
    bucket_name = input('버켓 이름을 적어주세요 : ') #jin-test11

    # ec2 경로 파일 유무 체크
    haha = ec2_check(check_dir)
    
    # s3 에 파일 넣기
    try:
        # 만약 s3 리스트가 존재할 경우에는 try 문 실행
        ec2_s3_del_up(haha, bucket_name)
    except:
        # 만약 s3 리스트가 비었을 경우에는 except 문 실행
        ec2_s3(haha, bucket_name)