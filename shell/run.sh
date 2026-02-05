#!/bin/bash

cd $(dirname $0)

BUILD_PATH=../
RELAY_BIN_PATH=${BUILD_PATH}/build/libelay

clean() {
  rm -rf ../build 1>/dev/null
  make clean -C ${BUILD_PATH} 1>/dev/null
}

wsDebugBuild() {
  rm -rf ../build 1>/dev/null
  make clean -C ${BUILD_PATH} 1>/dev/null
  cmake -S .. -B ../build -DCMAKE_BUILD_TYPE=Debug 1>/dev/null
  cmake --build ../build
  BUILD=debug make -C ${BUILD_PATH}
}

wsReleaseBuild() {
  rm -rf ../build 1>/dev/null
  make clean -C ${BUILD_PATH} 1>/dev/null
  cmake -S .. -B ../build -DCMAKE_BUILD_TYPE=Release 1>/dev/null
  cmake --build ../build
  BUILD=release make -C ${BUILD_PATH}
}

case "$1" in
  memcheck)
    wsDebugBuild
    echo "debug run (memcheck)"
    valgrind --tool=memcheck --leak-check=full --show-leak-kinds=all --track-origins=yes -s ${RELAY_BIN_PATH}
    ;;
  helgrind)
    wsDebugBuild
    echo "debug run (helgrind)"
    valgrind --tool=helgrind --history-level=approx -s ${RELAY_BIN_PATH}
    ;;
  debug)
    wsDebugBuild
    echo "debug run"
    #gdb --args ${RELAY_BIN_PATH}
    ${RELAY_BIN_PATH}
    ;;
  release)
    wsReleaseBuild
    echo "release run"
    ${RELAY_BIN_PATH}
    ;;
  clean)
    clean
    ;;
  *)
    wsReleaseBuild
    echo "release run"
    ${RELAY_BIN_PATH}
    ;;
esac
