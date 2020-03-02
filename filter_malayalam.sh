
#!/bin/bash
# Create an EC2 instance using the Ubuntu 18 AMI and at least 50 GB of storage.
# Then scp this directory to the EC2 instance and run this script with the parameters to copy over the S3 files and filter out malayalam content.
printf "\nArguments \n"
printf "========================================================================\n"
echo "AWS Key ID:                  " $1
echo "AWS Secret Key:              " $2
echo "S3 warc folder:              " $3
echo "S3 Out Directory:            " $4
printf "========================================================================\n"

S3_WARC_FOLDER=$3
S3_OUTPUT_DIRECTORY=$4

sudo apt install awscli -y

aws configure set aws_access_key_id $1
aws configure set aws_secret_access_key $2

sudo apt update
sudo apt install python3-pip -y
pip3 install --upgrade pip

pip3 install -r requirements.txt

#aws s3 cp $S3_WARC_FOLDER warcs/ --recursive

mkdir malayalam_filtered_html_body
mkdir unfiltered_heading_and_para
python3 process_warc_batch.py

aws s3 cp <tar_files> <s3 filtered op page>



