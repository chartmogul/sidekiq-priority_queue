Sidekiq Priority Queue
==============
Extends Sidekiq with support for queuing jobs with a fine grained priority and emulating multiple queues using a single Redis sorted set, ideal for multi-tenant applications.

The standard Sidekiq setup performs really well using Redis lists, but lists can only be strict FIFO queues, which can be hugely problematic when they process slowly and one user may need to wait hours behind a backlog of jobs.

Sidekiq Priority Queue offers a plug-in solution retaining the simplicity and performance of Sidekiq. The priority queue is a building block for emulating sub-queues (per tenant or user) by de-prioritising jobs according to how many jobs are already in this sub-queue.

Sources of inspiration are naturally Sidekiq itself, the fantastic Redis documentation, and https://github.com/gocraft/work

Installation
-----------------

    gem install sidekiq-priority_queue

Configuration
-----------------   
```
Sidekiq.configure_server do |config|
  config.options[:fetch] = Sidekiq::PriorityQueue::Fetch
end

Sidekiq.configure_client do |config|
  config.client_middleware do |chain|
    chain.add Sidekiq::PriorityQueue::Client
  end
end
```
Usage
-----------------   
```
class Worker
  include Sidekiq::Worker
  sidekiq_options priority: 1000
end
```
Alternatively, you can split jobs into subqueues (via a proc) which are deprioritised based on the subqueue size:
```
class Worker
  include Sidekiq::Worker

  # args[0] will take the `user_id` argument below, and assign priority dynamically.
  sidekiq_options subqueue: ->(args){ args[0] }

  def perform(user_id, other_args)
    # do jobs
  end
end
```

Development
-----------------
- Install [VirtualBox](https://www.virtualbox.org/wiki/Downloads) and [Vagrant](https://www.vagrantup.com/downloads.html)
- Start Vagrant with `vagrant up && vagrant ssh`
- Run `bundle install`
- Run the tests with `bundle exec rake`
