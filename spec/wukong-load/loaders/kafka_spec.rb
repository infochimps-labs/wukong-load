require 'spec_helper'
require 'kafka'

describe Wukong::Load::KafkaLoader do

  let(:loader)                             { Wukong::Load::KafkaLoader.new                                           }
  let(:loader_with_custom_topic)           { Wukong::Load::KafkaLoader.new(:topic           => 'custom'            ) }
  let(:loader_with_custom_topic_field)     { Wukong::Load::KafkaLoader.new(:topic_field     => '_custom_topic'     ) }
  let(:loader_with_custom_partition)       { Wukong::Load::KafkaLoader.new(:partition       => 1                   ) }
  let(:loader_with_custom_partition_field) { Wukong::Load::KafkaLoader.new(:partition_field => '_custom_partition' ) }

  let(:record)                             { {'text' => 'hi'                                  } }
  let(:record_with_topic_field)            { {'text' => 'hi', '_topic'            => 'custom' } }
  let(:record_with_custom_topic_field)     { {'text' => 'hi', '_custom_topic'     => 'custom' } }
  let(:record_with_partition_field)        { {'text' => 'hi', '_partition'        => 1        } }
  let(:record_with_custom_partition_field) { {'text' => 'hi', '_custom_partition' => 1        } }


  it "raises an error on setup if it can't connect to Kafka" do
    Kafka::MultiProducer.should_receive(:new).and_raise(StandardError)
    expect { processor(:kafka_loader) }.to raise_error(Wukong::Error)
  end
  
  context "with a Kafka available" do
    before do
      @producer = double()
      Kafka::MultiProducer.stub!(:new).and_return(@producer)
    end
    it_behaves_like 'a processor', :named => :kafka_loader

    it "produces an INFO log message on every write" do
      @producer.should_receive(:send)
      processor(:kafka_loader) do |proc|
        proc.log.should_receive(:info)
        proc.load(record)
      end
    end
    
  end
  
  context "routes" do
    context "all records" do
      it "to a default topic" do
        loader.topic_for(record).should == loader.topic
      end
      it "to a given topic" do
        loader_with_custom_topic.topic_for(record).should == 'custom'
      end
      it "to a default partition" do
        loader.partition_for(record).should == loader.partition
      end
      it "to a given partition" do
        loader_with_custom_partition.partition_for(record).should == 1
      end
    end
    context "records having a value for" do
      it "default topic field to the given topic" do
        loader.topic_for(record_with_topic_field).should == 'custom'
      end
      it "given topic field to the given topic" do
        loader_with_custom_topic.topic_for(record_with_topic_field).should == 'custom'
      end
      it "default partition field to the given partition" do
        loader.partition_for(record_with_partition_field).should == 1
      end
      it "given partition field to the given partition" do
        loader_with_custom_partition.partition_for(record_with_partition_field).should == 1
      end
    end
  end

end
