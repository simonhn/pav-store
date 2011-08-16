#core stuff
require 'rubygems'
require 'logger'  

#models
require './models'

#for xml fetch and parse
require 'rest_client'
require 'crack'
require 'nokogiri'
require 'chronic'

def configure
  $LOG = Logger.new('log/pavstore.log', 'monthly')
  #setup MySQL connection:  
  @config = YAML::load( File.open( 'config/settings.yml' ) )
  @connection = "#{@config['adapter']}://#{@config['username']}:#{@config['password']}@#{@config['host']}/#{@config['database']}";
  DataMapper::setup(:default, @connection)
  DataMapper.finalize
  #DataMapper::auto_migrate! 
  #result = RestClient.get "http://127.0.0.1:9393/v1/artist/1", {:accept => "text/xml"}
  #puts "#{result.code} : #{result}"
end

def get_program(programxml, d)
  if !programxml.nil?
    result = RestClient.get programxml
    xml = Crack::XML.parse(result)
    xml["schedule"]["programs"]["program"].each do |item|
      if item["day"]==d.strftime("%A")
        hat = item["time"]
        a = hat[0..1]
        b = hat[2..3]
        c = a+':'+b
        from = Chronic.parse('today '+ c)
        hours = Chronic.parse(item["duration"]).strftime("%H").to_i*3600
        minutes = Chronic.parse(item["duration"]).strftime("%M").to_i*60
        duration = hours+minutes
        to = from + duration
        if d > from && d < to
          #puts item["id"]
          #puts ' '
          return item["id"]
        end
      end
    end
  end
end

#Fetching xml, run through items and post to pav:api to store
def parse(id)
  file = Channel.get(id)
  #channels.each do |file|
  
    #puts file.channelname
    
    #fetching the xml
    begin
       result = RestClient.get file.channelxml
       xml = Crack::XML.parse(result)
       #puts file.channelxml.inspect
       
      rescue StandardError => e
        $LOG.info("Issue while fetching or processing xml for #{result} - error: #{e.backtrace}")  
        
      end
     
      xml["abcmusic_playout"]["items"]["item"].each do |item|
        if !item['artist']['artistname'].nil?
          #puts item['artist']['artistname']
          
          d = Time.parse(item['playedtime'])
          #triple j timestamp has utm zone indentifier, so changing to aest +10
          time = Time.new(d.year,d.month,d.day,d.hour,d.min,d.sec,"+10:00")
          program_id = get_program(file.programxml, time)
          item['program_id'] = program_id
          result = RestClient.post "http://#{@config['authuser']}:#{@config['authpass']}@96.126.96.51/v1/track", {:payload => {:channel => file.id,:item => item}}, :content_type => 'application/json'
          
           #result = RestClient.post "http://admin:#{@config['authpass']}@api.simonium.com/v1/track", {:payload => {:channel => file.id,:item => item}}, :content_type => 'application/json'
           #result = RestClient.post "http://admin:#{@config['authpass']}@127.0.0.1:4567/v1/track", {:payload => {:channel => file.id,:item => item}}, :content_type => 'application/json'
           
           #puts "#{result.code} : #{result}"
        end  
     end
    #end
end


configure
parse(ARGV[0])