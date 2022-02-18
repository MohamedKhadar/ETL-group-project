set -eu
#### CONFIGURATION SECTION ####
stack_name=group3
deployment_bucket=group3-deploybucket
#### CONFIGURATION SECTION ####
# Delete deploy dir if exists for a clean start
if [ -d ".deployment" ]; then rm -rf .deployment; fi
# Install dependencies from requirements.txt with container at python 3.9
docker run -v "$PWD":/var/task "public.ecr.aws/sam/build-python3.9" /bin/sh -c #"pip install -r requirements.txt -t ./.deployment/dependencies; exit"
# Zip all installed dependencies into a deployment package
cd ./.deployment/dependencies
zip -r ../lambda-package.zip .
# Merge 'app' directory with python code into the deployment package zip
cd ../../source
zip -gr ../.deployment/lambda-package.zip app
cd ..
# Package template and upload local resources (deployment package zip) to S3
# A unique S3 filename is automatically generated each time
aws cloudformation package --template-file cloudformation.yml --s3-bucket ${deployment_bucket} --output-template-file .deployment/cloudformation-packaged.yml
# Deploy template
aws cloudformation deploy --stack-name ${stack_name}-etl --template-file .deployment/cloudformation-packaged.yml --region eu-west-1 --capabilities CAPABILITY_IAM --parameter-overrides NamePrefix=${stack_name}