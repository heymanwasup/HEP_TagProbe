DIR=$(cd $(dirname ${BASH_SOURCE[0]})&&pwd)
export PYTHONPATH=$DIR/scripts/:$DIR/HEP_pyLib:$PYTHONPATH
if [[ ${ATLAS_LOCAL_ROOT_BASE} != "" ]];then
    setupATLAS
    lsetup "python 2.7.9p1-x86_64-slc6-gcc49"
    lsetup "root 6.08.06-x86_64-slc6-gcc49-opt"
fi