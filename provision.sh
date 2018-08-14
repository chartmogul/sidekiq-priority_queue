sudo apt-get -y update
sudo apt-get -y upgrade

sudo apt-get -y install redis-server ruby-dev build-essential zlib1g-dev libsqlite3-dev nodejs

cd /vagrant
sudo gem install bundler
