#!/usr/bin/env bash

set -ex

script_name=$0

print_usage() {
    cat <<EOF
Usage: $script_name [-h|--help]
                    [--proxy proxy address]
                    [--path distributive path]
                    [--jar jar file]
                    [--kind spark|spyt]
                    [--enable-nbd]
                    [--replication-factor number]

  --proxy: YT proxy address
  --path: Path to distributive on Cypress
  --jar: Extra jar to include, may be repeated, applicable only to spark kind
  --kind: distributive type: spark, spyt or livy
  --enable-nbd: use squashfs layer as nbd image
  --replication-factor: replication factor for nbd layers (default 10)

EOF
    exit 0
}

replication_factor="10"

# Parse options
while [[ $# -gt 0 ]]; do
    key="$1"
    case $key in
        --proxy)
        proxy="$2"
        shift 2
        ;;
        --path)
        path="$2"
        shift 2
        ;;
        --kind)
        kind="$2"
        shift 2
        ;;
        --enable-nbd)
        enable_nbd=1
        shift
        ;;
        --replication-factor)
        replication_factor="$2"
        shift 2
        ;;
        --jar)
        jars="$jars $2"
        shift 2
        ;;
        -h|--help)
        print_usage
        shift
        ;;
        *)  # unknown option
        echo "Unknown argument $1"
        print_usage
        ;;
    esac
done

if [[ -z "$proxy" ]]; then
  echo "No YT proxy address specified"
  exit 1
fi

if [[ -z "$path" ]]; then
  echo "Path to spark distributive tgz must be specified"
  exit 1
fi

case $kind in
    spark|spyt|livy) echo "Converting $kind distributive to squashfs";;
    *)
    echo "Kind must be specified and should be spark or spyt"
    exit 1
    ;;
esac

libroot="/usr/lib"
filename=${path##*/}

tmpdir=$(mktemp -d)
trap 'rm -r $tmpdir' EXIT

localfile="$tmpdir/$filename"
yt --proxy $proxy read-file $path > $localfile
distroot="$tmpdir/root$libroot"
mkdir -p $distroot
case $kind in
    spark|livy)
      mkdir "$distroot/$kind"
      tar xzf $localfile --strip-components 1 -C "$distroot/$kind"
      if [[ $kind = "spark" && -n "$jars" ]]; then
        for jar in $jars
        do
          jar_filename=${jar##*/}
          jar_localfile="$tmpdir/$jar_filename"
          yt --proxy $proxy read-file $jar > $jar_localfile
          mv $jar_localfile "$distroot/$kind/jars/"
        done
      fi
      sqfsfilename=${filename/.tgz/.squashfs}
      ;;
    spyt)
      unzip $localfile -d $distroot
      mv "$distroot/spyt-package" "$distroot/$kind"
      sparkpatchjar="$libroot/$kind/jars/$(basename $distroot/$kind/jars/*spark-yt-spark-patch*)"
      javaagent_opt="-javaagent:$sparkpatchjar"
      echo "$javaagent_opt" > $distroot/$kind/conf/java-opts
      sqfsfilename=${filename/.zip/.squashfs}
      ;;
esac

sqfsfilepath="$tmpdir/$sqfsfilename"
mksquashfs "$tmpdir/root" $sqfsfilepath

targetpath=${path/"$filename"/"$sqfsfilename"}
if [ $enable_nbd ]; then
  yt --proxy $proxy create --type file \
  --attributes "{primary_medium=ssd_blobs;account=sys;replication_factor=$replication_factor;access_method=nbd}" \
  --path $targetpath
fi
yt --proxy $proxy write-file $targetpath < $sqfsfilepath
yt --proxy $proxy set $targetpath/@filesystem squashfs
