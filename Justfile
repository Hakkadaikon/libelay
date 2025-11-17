clean:
    rm -rf build

fmt:
    clang-format -i $(find ./src -name '*.[ch]')

release-build:
    rm -rf build
    cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
    cmake --build build

debug-build:
    clean
    cmake -S . -B build -DCMAKE_BUILD_TYPE=Debug
    cmake --build build

