# Reload Script Examples

These are examples of reload scripts. The script highly depends on how to reset relevant states in the underlying storage.

Azure NFS:
```
mkdir -p ~/nfs
sudo umount ~/nfs
sudo sysctl vm.drop_caches=3
sudo mount -o sec=sys,vers=3,nolock,proto=tcp ${AZURE_STORAGE_ACCOUNT}.blob.core.windows.net:/${AZURE_STORAGE_ACCOUNT}/nfs ~/nfs
sudo chmod 777 ~/nfs
(TIMEFORMAT=%R; time echo `cat ~/nfs/random.txt | wc -l` > /dev/null)
sleep 10
echo "reload_nfs.sh completed"
```

SSD:
```
sudo sysctl vm.drop_caches=3
(TIMEFORMAT=%R; time echo `cat ~/ssd/random.txt | wc -l`)
sleep 10
echo "reload_local completed"
```
