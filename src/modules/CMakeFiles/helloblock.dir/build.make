# CMAKE generated file: DO NOT EDIT!
# Generated by "Unix Makefiles" Generator, CMake Version 3.14

# Delete rule output on recipe failure.
.DELETE_ON_ERROR:


#=============================================================================
# Special targets provided by cmake.

# Disable implicit rules so canonical targets will work.
.SUFFIXES:


# Remove some rules from gmake that .SUFFIXES does not remove.
SUFFIXES =

.SUFFIXES: .hpux_make_needs_suffix_list


# Suppress display of executed commands.
$(VERBOSE).SILENT:


# A target that is always out of date.
cmake_force:

.PHONY : cmake_force

#=============================================================================
# Set environment variables for the build.

# The shell in which to execute make rules.
SHELL = /bin/sh

# The CMake executable.
CMAKE_COMMAND = /Applications/CLion.app/Contents/bin/cmake/mac/bin/cmake

# The command to remove a file.
RM = /Applications/CLion.app/Contents/bin/cmake/mac/bin/cmake -E remove -f

# Escaping for special characters.
EQUALS = =

# The top-level source directory on which CMake was run.
CMAKE_SOURCE_DIR = "/Users/zhouxiaojie/open source project/redis-4.0.0"

# The top-level build directory on which CMake was run.
CMAKE_BINARY_DIR = "/Users/zhouxiaojie/open source project/redis-4.0.0"

# Include any dependencies generated for this target.
include src/modules/CMakeFiles/helloblock.dir/depend.make

# Include the progress variables for this target.
include src/modules/CMakeFiles/helloblock.dir/progress.make

# Include the compile flags for this target's objects.
include src/modules/CMakeFiles/helloblock.dir/flags.make

src/modules/CMakeFiles/helloblock.dir/helloblock.c.o: src/modules/CMakeFiles/helloblock.dir/flags.make
src/modules/CMakeFiles/helloblock.dir/helloblock.c.o: src/modules/helloblock.c
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green --progress-dir="/Users/zhouxiaojie/open source project/redis-4.0.0/CMakeFiles" --progress-num=$(CMAKE_PROGRESS_1) "Building C object src/modules/CMakeFiles/helloblock.dir/helloblock.c.o"
	cd "/Users/zhouxiaojie/open source project/redis-4.0.0/src/modules" && /Applications/Xcode.app/Contents/Developer/Toolchains/XcodeDefault.xctoolchain/usr/bin/cc $(C_DEFINES) $(C_INCLUDES) $(C_FLAGS) -o CMakeFiles/helloblock.dir/helloblock.c.o   -c "/Users/zhouxiaojie/open source project/redis-4.0.0/src/modules/helloblock.c"

src/modules/CMakeFiles/helloblock.dir/helloblock.c.i: cmake_force
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green "Preprocessing C source to CMakeFiles/helloblock.dir/helloblock.c.i"
	cd "/Users/zhouxiaojie/open source project/redis-4.0.0/src/modules" && /Applications/Xcode.app/Contents/Developer/Toolchains/XcodeDefault.xctoolchain/usr/bin/cc $(C_DEFINES) $(C_INCLUDES) $(C_FLAGS) -E "/Users/zhouxiaojie/open source project/redis-4.0.0/src/modules/helloblock.c" > CMakeFiles/helloblock.dir/helloblock.c.i

src/modules/CMakeFiles/helloblock.dir/helloblock.c.s: cmake_force
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green "Compiling C source to assembly CMakeFiles/helloblock.dir/helloblock.c.s"
	cd "/Users/zhouxiaojie/open source project/redis-4.0.0/src/modules" && /Applications/Xcode.app/Contents/Developer/Toolchains/XcodeDefault.xctoolchain/usr/bin/cc $(C_DEFINES) $(C_INCLUDES) $(C_FLAGS) -S "/Users/zhouxiaojie/open source project/redis-4.0.0/src/modules/helloblock.c" -o CMakeFiles/helloblock.dir/helloblock.c.s

# Object files for target helloblock
helloblock_OBJECTS = \
"CMakeFiles/helloblock.dir/helloblock.c.o"

# External object files for target helloblock
helloblock_EXTERNAL_OBJECTS =

src/modules/helloblock.so: src/modules/CMakeFiles/helloblock.dir/helloblock.c.o
src/modules/helloblock.so: src/modules/CMakeFiles/helloblock.dir/build.make
src/modules/helloblock.so: src/modules/CMakeFiles/helloblock.dir/link.txt
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green --bold --progress-dir="/Users/zhouxiaojie/open source project/redis-4.0.0/CMakeFiles" --progress-num=$(CMAKE_PROGRESS_2) "Linking C shared library helloblock.so"
	cd "/Users/zhouxiaojie/open source project/redis-4.0.0/src/modules" && $(CMAKE_COMMAND) -E cmake_link_script CMakeFiles/helloblock.dir/link.txt --verbose=$(VERBOSE)

# Rule to build all files generated by this target.
src/modules/CMakeFiles/helloblock.dir/build: src/modules/helloblock.so

.PHONY : src/modules/CMakeFiles/helloblock.dir/build

src/modules/CMakeFiles/helloblock.dir/clean:
	cd "/Users/zhouxiaojie/open source project/redis-4.0.0/src/modules" && $(CMAKE_COMMAND) -P CMakeFiles/helloblock.dir/cmake_clean.cmake
.PHONY : src/modules/CMakeFiles/helloblock.dir/clean

src/modules/CMakeFiles/helloblock.dir/depend:
	cd "/Users/zhouxiaojie/open source project/redis-4.0.0" && $(CMAKE_COMMAND) -E cmake_depends "Unix Makefiles" "/Users/zhouxiaojie/open source project/redis-4.0.0" "/Users/zhouxiaojie/open source project/redis-4.0.0/src/modules" "/Users/zhouxiaojie/open source project/redis-4.0.0" "/Users/zhouxiaojie/open source project/redis-4.0.0/src/modules" "/Users/zhouxiaojie/open source project/redis-4.0.0/src/modules/CMakeFiles/helloblock.dir/DependInfo.cmake" --color=$(COLOR)
.PHONY : src/modules/CMakeFiles/helloblock.dir/depend

