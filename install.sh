#!/bin/bash
PATH=/www/server/panel/pyenv/bin:/bin:/sbin:/usr/bin:/usr/sbin:/usr/local/bin:/usr/local/sbin:~/bin
export PATH

# 插件安装目录
install_path=/www/server/panel/plugin/aliyundrive_backup

# 安装
Install()
{
    echo '正在安装阿里云盘备份插件...'
    #==================================================================
    # /www/server/panel/pyenv/bin/pip install requests
    #==================================================================
    echo '================================================'
    echo '阿里云盘备份插件安装完成'
}

# 卸载
Uninstall()
{
    rm -rf $install_path
}

# 操作判断
if [ "${1}" == 'install' ];then
    Install
elif [ "${1}" == 'uninstall' ];then
    Uninstall
else
    echo 'Error!';
fi

