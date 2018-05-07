BASE_IMAGE_NAME=nslrpi/test-spark
OUTPUT_TAG=$BASE_IMAGE_NAME:latest

function buildImage() {
    docker build -t $BASE_IMAGE_NAME -f Dockerfile . && echo "build base image"
    docker tag $BASE_IMAGE_NAME $BASE_IMAGE_NAME:latest
}

buildImage