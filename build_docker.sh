aws ecr get-login-password --region eu-west-1 | docker login --username AWS --password-stdin 249959970268.dkr.ecr.eu-west-1.amazonaws.com

docker build -t rpscrape .

docker tag rpscrape:latest 249959970268.dkr.ecr.eu-west-1.amazonaws.com/rpscrape:latest

docker push 249959970268.dkr.ecr.eu-west-1.amazonaws.com/rpscrape:latest