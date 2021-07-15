
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
    client = boto3.client('s3')

    #파일 업로드
    for file in lists:
        # 경로에서 s3 에 업로드할 파일 이름 생성
        print(file)
        hehe = file.split('/')[-1]

        #파일 s3 에 업로드 (업로드 날자별 파티셔닝 생성하여 업로드)
        client.put_object(Bucket=bucket_name,Body=file, Key=(time.strftime('%Y-%m-%d', time.localtime(time.time())))+'/'+hehe) # 첫번째 매개변수 : S3 버킷 이름, #두번째 매개변수 : 로컬에서 올릴 파일이름,  # 세번째 매개변수 : 버킷에 저장될 파일 이름


if __name__ == "__main__":

    # requirements 입력
    check_dir = input('파일 경로를 입력해주세요 : ') #/home/jinchoe/jin_test  # 파일 체크할 ec2 경로 입력
    bucket_name = input('버켓 이름을 적어주세요 : ') #jin-test11

    # ec2 경로 파일 유무 체크
    haha = ec2_check(check_dir)
    
    # s3 에 파일 넣기
    ec2_s3(haha, bucket_name)
