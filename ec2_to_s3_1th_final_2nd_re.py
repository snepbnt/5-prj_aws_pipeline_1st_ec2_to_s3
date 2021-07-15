
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


# 버킷 내 경로 체크 및 선택 함수
def check_s3_dir(bucket_name):

    # s3 인증 및 파일 리스트 가져오기위한 절차
    paginator = client.get_paginator('list_objects_v2')
    response_iterator = paginator.paginate(Bucket=bucket_name)

    s3_lists = [s3_lists for page in response_iterator for s3_lists in page['Contents'] for list in range(len(s3_lists))]

    # 버킷 내 경로 리스트 만들기
    s3_dir_list = []
    for i in range(len(s3_lists)):
        contents = s3_lists[i]['Key']
        if contents.find('.csv') != -1:
            haha = contents.split('/')
            haha.pop()
            haha = '/'.join(haha) + '/' # pop 으로 csv 파일을 제거할 경우 뒤에 '/' 경로도 없어져서 추가적으로 '/' 를 더해줌
            s3_dir_list.append(haha)

        elif contents.find('.csv') == -1:
            haha = contents.split('/')
            haha = '/'.join(haha)
            s3_dir_list.append(haha)

    s3_dir_list = sorted(list(set(s3_dir_list)))

    # 리스트 메뉴를 만들고
    select_Menu = Enum('select_Menu', s3_dir_list)
    s = [f'({m.value}){m.name}' for m in select_Menu]
    print(*s, sep='  ', end='')
    print("  없는 경우에는 0번을 입력해주세요")
    n = int(input(' : '))
    
    # 번호에 맞추어 리스트를 선택하기 
    if 1 <= n <= len(select_Menu):
        print(select_Menu(n).name)
        return select_Menu(n).name
    # 충족하는 경로가 없는 경우
    else:
        yes_or_yes = (input('새로운 경로를 만드시겠습니까? (yes 를 입력해주세요) : '))
        return yes_or_yes



# 새롭게 파일을 업로드 하는 함수
def ec2_s3_cre(file_lists, bucket_name, client, directions):

    print(directions)
    # 버킷 내 1차 경로 체크
    paginator = client.get_paginator('list_objects_v2')
    response_iterator = paginator.paginate(Bucket='s3-an2-si-bplace-temp')

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
    print("1 : 사용자 지정경로 / 2 : 날짜 경로(당일날짜 자동 생성) / 3 : 생성하지 않음(버킷에 그대로 업로드) ")
    choice_num = int(input("번호를 입력해주세요 : "))

    new_file = []
    if choice_num == 1: # 사용자 지정 경로
        # 리스트 메뉴를 만들기
        select_Menu = Enum('select_Menu', dir_1st)
        s = [f'({m.value}){m.name}' for m in select_Menu]
        print(*s, sep='  ', end='')
        print("  원하는 경로를 선택하세요")
        n = int(input(' 번호를 입력해주세요 (원하는 경로가 없으면 0 을 입력해주세요) : '))

        if n != 0: # 원하는 경로가 있을 경우
            dir_1st = select_Menu(n).name
            # 1차 경로 아래 저장할 세부 경로 작성
            input_dir = str(input("하위 세부 경로를 작성하세요 : "))
            user_dir = dir_1st + '/'+ input_dir
            print(user_dir)
            
            for file in file_lists:
                # 경로에서 s3 에 업로드할 파일 이름 생성
                hehe = file.split('/')[-1]
                # 파일 s3 에 업로드
                client.put_object(Bucket=bucket_name, Body=file, Key= user_dir + '/' + hehe)  # 첫번째 매개변수 : S3 버킷 이름, #두번째 매개변수 : 로컬에서 올릴 파일이름,  # 세번째 매개변수 : 버킷에 저장될 파일 이름
                new_file.append(hehe)
        else: # 원하는 경로가 없을 경우 새로 작성
            user_dir = str(input("경로를 작성하세요 : "))
            for file in file_lists:
                # 경로에서 s3 에 업로드할 파일 이름 생성
                hehe = file.split('/')[-1]
                # 파일 s3 에 업로드
                client.put_object(Bucket=bucket_name, Body=file, Key=user_dir + '/' + hehe)  # 첫번째 매개변수 : S3 버킷 이름, #두번째 매개변수 : 로컬에서 올릴 파일이름,  # 세번째 매개변수 : 버킷에 저장될 파일 이름
                new_file.append(hehe)

    elif choice_num == 2: # 날짜 경로 (당일 날짜 자동 생성)
        # 리스트 메뉴를 만들기
        select_Menu = Enum('select_Menu', dir_1st)
        s = [f'({m.value}){m.name}' for m in select_Menu]
        print(*s, sep='  ', end='')
        print("  원하는 경로를 선택하세요")
        n = int(input(' 번호를 입력해주세요 (원하는 경로가 없으면 0 을 입력해주세요) : '))

        if n != 0: # 원하는 경로가 있을 경우
            dir_1st = select_Menu(n).name

            # 파일 업로드
            for file in file_lists:
                # 경로에서 s3 에 업로드할 파일 이름 생성
                hehe = file.split('/')[-1]
                # 파일 s3 에 업로드 (업로드 날자별 파티셔닝 생성하여 업로드)
                client.put_object(Bucket=bucket_name, Body=file, Key=(dir_1st + '/' + time.strftime('%Y-%m-%d', time.localtime(time.time()))) + '/' + hehe)  # 첫번째 매개변수 : S3 버킷 이름, #두번째 매개변수 : 로컬에서 올릴 파일이름,  # 세번째 매개변수 : 버킷에 저장될 파일 이름
                new_file.append(hehe)

        else: # 원하는 경로가 없을 경우 새로 작성
            user_dir = str(input("경로를 작성하세요 : "))
            for file in file_lists:
                # 경로에서 s3 에 업로드할 파일 이름 생성
                hehe = file.split('/')[-1]
                # 파일 s3 에 업로드
                client.put_object(Bucket=bucket_name, Body=file, Key=(user_dir + '/' + time.strftime('%Y-%m-%d', time.localtime(time.time()))) + '/' + hehe)  # 첫번째 매개변수 : S3 버킷 이름, #두번째 매개변수 : 로컬에서 올릴 파일이름,  # 세번째 매개변수 : 버킷에 저장될 파일 이름
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
def ec2_s3_del_up(file_lists, bucket_name, client, directions):

    # s3 인증 및 파일 리스트 가져오기위한 절차
    paginator = client.get_paginator('list_objects_v2')
    response_iterator = paginator.paginate(Bucket=bucket_name)

    # s3 경로 파일 저장된 파일 리스트 추출 (list comprehension 3중 for 문)
    s3_lists = [s3_lists for page in response_iterator for s3_lists in page['Contents'] for list in range(len(s3_lists))]

    s3_file_list = []
    for i in range(len(s3_lists)):
        s3_file_list.append(s3_lists[i]['Key'])

    # 중복된 리스트 부분을 제거한 후 다시 리스트로 만들기
    s3_file_list = sorted(list(set(s3_file_list))) # 중복을 제거하기 위해 set() 함수 사용하여 set으로 만들고 난 후 list화
    s3_file_list = [s3_file_list[i] for i in range(len(s3_file_list)) if s3_file_list[i].find('.csv') != -1]
    print(s3_file_list)
    print('')


    update_file = []
    stay_file = []

    # 리스트를 비교하여 s3 파일 업데이트 및 추가하기
    for file in file_lists:
        # 절대경로에서 파일이름 부분만 가져오기
        file = file.split('/')[-1]
        # 넣고자 하는 경로와 합쳐서 기존에 파일존재하는지 비교
        file = directions + file

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
    check_dir = input('파일 경로를 입력해주세요 : ')
    file_list = ec2_check(check_dir)

    # 버킷 선택
    print("업로드할 버킷을 선택해주세요")
    selects = select_menu()
    selects_bucket = selects.name
    print(selects_bucket)

    # 버킷 내 경로 유무체크
    directions = check_s3_dir(selects_bucket)
    if directions != 'yes':
        # 경로 존재할 경우(경로 리턴) - 해당 경로에 데이터 넣거나 업데이트
        ec2_s3_del_up(file_list, selects_bucket, client, directions)
        print("작업을 완료했습니다.")
    elif directions == 'yes':
        # 경로가 존재하지 않을 경우(yes 리턴) - 해당 경로를 ec2 의 경로를 참고하여 만들기
        ec2_s3_cre(file_list, selects_bucket, client, check_dir)
        print("작업을 완료했습니다.")




