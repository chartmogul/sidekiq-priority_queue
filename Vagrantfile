Vagrant.configure(2) do |config|
  config.vm.box = 'ubuntu/xenial64'
  config.vm.hostname = 'sidekiq-priority'
  config.vm.network 'private_network', ip: '172.28.128.120'
  config.vm.provider 'virtualbox' do |vb|
    vb.memory = 1024
    vb.cpus = 1
  end
  config.vm.provision 'shell', privileged: false, path: 'provision.sh'
end
