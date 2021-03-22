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

function build_images_and_push {
    echo "------ Building local docker images"
    docker-compose build > /dev/null
    echo "------ Local docker images build"

    echo "------ Pushing local images to docker hub"
    docker-compose push > /dev/null
    echo "------ Pushed images to docker hub"
}

echo "------ Building Python SDK and base image"
build_python_sdk_image
echo "------ Python SDK base image built"

echo "------ Building Statefun project and base image"
build_statefun_image
echo "------ Statefun base image built"

echo "---------- Building TPC project"
cd ${CURRENT_DIR}/tpc
build_images_and_push
echo "---------- Building original project"
cd ${CURRENT_DIR}/original
build_images_and_push
echo "---------- Building Sagas project"
cd ${CURRENT_DIR}/sagas
build_images_and_push
echo "---------- Building internal invocations project"
cd ${CURRENT_DIR}/internal-invocations
build_images_and_push
