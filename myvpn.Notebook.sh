docker run --device /dev/net/tun --cap-add NET_ADMIN -ti -v $HOME/.easyconn:/root/.easyconn -p 127.0.0.1:1080:1080 -e EC_VER=7.6.7 hagb/docker-easyconnect:cli
# port
docker run --device /dev/net/tun --cap-add NET_ADMIN -ti -e PASSWORD=xxxx -v $HOME/.ecdata:/root -p 127.0.0.1:15901:5901 -p 127.0.0.1:10180:1080 hagb/docker-easyconnect:7.6.7
docker run --device /dev/net/tun --name=EasyConnect_CLI_ARM --cap-add NET_ADMIN -ti -p 1080:1080 -p 8888:8888 -e EC_VER=7.6.3 -e CLI_OPTS="-d https://vpn.pbwear.com -u  ying.shengfeng -p shengfeng8877" hagb/docker-easyconnect
# New Xtigervnc server 'a7190c2427a7:1 (root)' on port 5901 for display :1.
# Use xtigervncviewer -SecurityTypes VncAuth,TLSVnc -passwd /tmp/tigervnc.cj7AGF/passwd a7190c2427a7:1 to connect to the VNC server.
# 
brew install tigervnc

# connect
xtigervncviewer -SecurityTypes VncAuth,TLSVnc -passwd /tmp/tigervnc.cj7AGF/passwd a7190c2427a7:1

vncviewer -SecurityTypes VncAuth,TLSVnc -passwd /tmp/tigervnc.cj7AGF/passwd a7190c2427a7:1

docker run --device /dev/net/tun --cap-add NET_ADMIN -ti -e PASSWORD=xxxx -v $HOME/.ecdata:/root -p 127.0.0.1:5901:5901 -p 127.0.0.1:1080:1080 hagb/docker-easyconnect:7.6.7
# 报错：The VPN server software is too low and does not support downloading client.
# Use xtigervncviewer -SecurityTypes VncAuth,TLSVnc -passwd /tmp/tigervnc.JumaYe/passwd cfcec1441f99:1 to connect to the VNC server.
docker run --device /dev/net/tun --cap-add NET_ADMIN -ti -e PASSWORD=xxxx -v $HOME/.ecdata:/root -p 127.0.0.1:5901:5901 -p 127.0.0.1:1080:1080 hagb/docker-easyconnect:7.6.3
# 7.6.3：适用于连接 <7.6.7 版本的 EasyConnect 服务端。
# 7.6.7：适用于连接 >= 7.6.7 版本的 EasyConnect 服务端。
# peacebird 7.6.6 https://vpn.pbwear.com
docker run --device /dev/net/tun --cap-add NET_ADMIN -ti -e PASSWORD=xxxx -v $HOME/.ecdata:/root -p 127.0.0.1:5901:5901 -p 127.0.0.1:1080:1080 -e DISABLE_PKG_VERSION_XML=1 hagb/docker-easyconnect:7.6.3