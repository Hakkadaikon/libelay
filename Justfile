clean:
    rm -rf build

release-build:
    rm -rf build
    cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
    cmake --build build

debug-build:
    clean
    cmake -S . -B build -DCMAKE_BUILD_TYPE=Debug
    cmake --build build

