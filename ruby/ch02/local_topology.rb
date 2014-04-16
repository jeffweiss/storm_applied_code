require 'red_storm'

class CommitFeedListener < RedStorm::DSL::Spout
  output_fields :commit
  on_init do
    @commits = File.readlines('changelog.txt')
  end

  on_send :emit => false do
    @commits.each { |c| unreliable_emit(c.strip) }
  end
end

class EmailExtractor < RedStorm::DSL::Bolt
  output_fields :email

  on_receive do |tuple|
    tuple[:commit].split[1]
  end
end

class EmailCounter < RedStorm::DSL::Bolt
  on_init do
    @counts = Hash.new(0)
  end

  on_receive do |tuple|
    email = tuple[:email]
    @counts[email] += 1
    print_counts
  end

  def print_counts
    @counts.keys.each do |email|
      puts "%s has count of %s" % [email, @counts[email]]
    end
  end
end

class LocalTopology < RedStorm::DSL::Topology
  spout CommitFeedListener

  bolt EmailExtractor do
    source CommitFeedListener, :shuffle
  end

  bolt EmailCounter do
    source EmailExtractor, :fields => :email
  end

  configure do
    set "topology.debug", true
  end

  on_submit do |env|
    if env == :local
      sleep(2*60)
      cluster.shutdown
    end
  end
end
