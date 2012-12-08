#!/bin/bash
NAME=qfs-mapred
VERSION=1.0
BUILD_ROOT=~/
SOURCE_DIR=~/
INSTALL_PREFIX=/opt/hurdad


#PREP


#BUILD


#INSTALL
#sudo rm -rf $BUILD_ROOT$NAME_$VERSION
#mkdir -p $BUILD_ROOT$NAME"_"$VERSION$INSTALL_PREFIX/bin

install -m 755 $SOURCE_DIR/qfs_mapred/src/kvsorter  $BUILD_ROOT$NAME"_"$VERSION$INSTALL_PREFIX/bin
install -m 755 $SOURCE_DIR/qfs_mapred/src/mapper_to_qfs_partitions  $BUILD_ROOT$NAME"_"$VERSION$INSTALL_PREFIX/bin
install -m 755 $SOURCE_DIR/qfs_mapred/src/mapper_worker  $BUILD_ROOT$NAME"_"$VERSION$INSTALL_PREFIX/bin
install -m 755 $SOURCE_DIR/qfs_mapred/src/reducer_worker  $BUILD_ROOT$NAME"_"$VERSION$INSTALL_PREFIX/bin
install -m 755 $SOURCE_DIR/qfs_mapred/src/sorter_worker  $BUILD_ROOT$NAME"_"$VERSION$INSTALL_PREFIX/bin
install -m 755 $SOURCE_DIR/qfs_mapred/src/qfs_mapred_submit  $BUILD_ROOT$NAME"_"$VERSION$INSTALL_PREFIX/bin

#ADD DEBIAN/control
mkdir -p $BUILD_ROOT$NAME"_"$VERSION/DEBIAN
cp $SOURCE_DIR/qfs_mapred/DEBIAN/control $BUILD_ROOT$NAME"_"$VERSION/DEBIAN/control

#MODIFY
sudo chown -R root:root $BUILD_ROOT$NAME"_"$VERSION

#BUILD DEB
dpkg-deb --build $BUILD_ROOT$NAME"_"$VERSION
mv $BUILD_ROOT$NAME"_"$VERSION.deb .
