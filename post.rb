#core stuff
require 'rubygems'
require 'logger'  

#models
require './models'

#for xml fetch and parse
require 'rest_client'
require 'crack'
require 'nokogiri'

def configure
  #setup MySQL connection:  
  @config = YAML::load( File.open( 'config/settings.yml' ) )
  @connection = "#{@config['adapter']}://#{@config['username']}:#{@config['password']}@#{@config['host']}/#{@config['database']}";
  DataMapper::setup(:default, @connection)
  #DataMapper.auto_upgrade!
  #DataMapper::auto_migrate! 
  #result = RestClient.get "http://127.0.0.1:9393/v1/artist/1", {:accept => "text/xml"}
  #puts "#{result.code} : #{result}"
end

#Fetching xml, run through items and post to pav:api to store
def parse
  channels = Channel.all
  channels.each do |file|
  
    #puts file.channelname
    
    #fetching the xml
    begin
       result = RestClient.get file.channelxml
       xml = Crack::XML.parse(result)
       puts file.channelxml.inspect
       
      rescue => e
        $LOG.info("Issue while fetching or processing xml for #{result} - error: #{e.backtrace}")  
        raise StandardError, "An xml error has occurred - #{e}", e.backtrace
      end
     
      xml["abcmusic_playout"]["items"]["item"].each do |item|
        if !item['artist']['artistname'].nil?
           result = RestClient.post "http://admin:#{@config['authpass']}@api.simonium.com/v1/track", {:payload => {:channel => file.id,:item => item}}, :content_type => 'application/json'
           #result = RestClient.post "http://admin:#{@config['authpass']}@127.0.0.1:9393/v1/track", {:payload => {:channel => file.id,:item => item}}, :content_type => 'application/json'
           
           #puts "#{result.code} : #{result}"
        end  
     end
    end
end


configure
parse