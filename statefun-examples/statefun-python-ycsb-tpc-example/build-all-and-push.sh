CURRENT_DIR=$(pwd)
BASE_DIR="${CURRENT_DIR}/../.."

function build_python_sdk_image {
    cd "${BASE_DIR}/statefun-python-sdk/"
    echo "--- Building project"
    ./build-distribution.sh > /dev/null
    echo "--- Building image"
    docker-compose build > /dev/null
}

function build_statefun_image {
    cd "${BASE_DIR}"
    echo "--- Packaging project"
    mvn clean package > /dev/null
    cd "${BASE_DIR}/tools/docker"
    echo "--- Building image"
    ./build-stateful-functions.sh > /dev/null
}

echo "------ Building Python SDK and base image"
build_python_sdk_image
echo "------ Python SDK base image built"

echo "------ Building Statefun project and base image"
build_statefun_image
echo "------ Statefun base image built"

cd ${CURRENT_DIR}
echo "------ Building local docker images"
docker-compose build > /dev/null
echo "------ Local docker images build"

echo "------ Pushing local images to docker hub"
docker-compose push > /dev/null
echo "------ Pushed images to docker hub"

