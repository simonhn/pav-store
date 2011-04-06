#core stuff
require 'rubygems'
require 'logger'  

#models
require './models'

#for xml fetch and parse
require 'rest_client'
require 'crack'
#require 'nokogiri'

#utf 16 to 8 stuff
require 'kconv'
require 'rchardet'

#musicbrainz stuff
require 'rbrainz'
include MusicBrainz

def configure
  #setup MySQL connection:  
  @config = YAML::load( File.open( 'config/settings.yml' ) )
  @connection = "#{@config['adapter']}://#{@config['username']}:#{@config['password']}@#{@config['host']}/#{@config['database']}";
  DataMapper::setup(:default, @connection)
  DataMapper.auto_upgrade!
  #DataMapper::auto_migrate! 
  
  #setup logging
  #DataMapper::Logger.new('log/datamapper.log', :debug)
  $LOG = Logger.new('log/pavstore.log', 'monthly')
end


#Fetching xml, parsing and storing it to db
def parse
  channels = Channel.all
  channels.each do |file|
    #puts file.channelname
    
    #fetching the xml
    begin
    result = RestClient.get file.channelxml
    
      #jjj's feed is encoded in utf-16, so we convert into utf-8
    
      utf8string = result.toutf8.sub("utf-16","utf-8") 
      xml = Crack::XML.parse(utf8string)
      rescue => e
        $LOG.info("Issue while fetching or processing xml for #{file.channelname} - error: #{e.backtrace}")  
        raise StandardError, "An xml error has occurred - #{e}", e.backtrace
    end
    if (file.id=4)
      $LOG.info('triple j')
    end
    #jjj's feed only contains one item, so result is not an array of item
    if (xml["abcmusic_playout"]["items"]["item"].length < 15) # !> redefine new
      if (Play.count(:playedtime =>xml["abcmusic_playout"]["items"]["item"].first.to_a[1].to_s, :channel_id=>file.id)==0)
        store_hash(xml["abcmusic_playout"]["items"]["item"], file.id)
      end
    else
      #'normal' behavior
      xml["abcmusic_playout"]["items"]["item"].each do |item|
        if !item['artist']['artistname'].nil? && Play.count(:playedtime => item['playedtime'], :channel_id=>file.id)==0
          store_hash(item, file.id)
        end
      end
    end      
  end
end

#Method that stores each playitem to database. Index is the id of the channel to associate the play with
def store_hash(item, index)
  #there can be multiple artist seperated by '+' so we split them
  artist_array = item['artist']['artistname'].split("+")
  artist_array.each{ |artist_item|
    
    begin
      #for each item, lookup in musicbrainz. Returns hash with mbids for track, album and artist if found
      mbid_hash = mbid_lookup(artist_item.strip, item['title'], item['album']['albumname'])
      rescue => e
        $LOG.info("Issue while processing #{artist_item.strip} - #{item['title']} - #{item['album']['albumname']}")  
        raise StandardError, "A musicbrainz error has occurred - #{e}", e.backtrace
    end
    
    #ARTIST
    if !mbid_hash["artistmbid"].nil?
      @artist = Artist.first_or_create({:artistmbid => mbid_hash["artistmbid"]},{:artistmbid => mbid_hash["artistmbid"],:artistname => artist_item.strip, :artistnote => item['artist']['artistnote'], :artistlink => item['artist']['artistlink']})
    else
      @artist = Artist.first_or_create({:artistname => artist_item.strip},{:artistname => artist_item.strip, :artistnote => item['artist']['artistnote'], :artistlink => item['artist']['artistlink']})
    end

    if @artist.save
      #ALBUM
      #creating and saving album if not exists
      #there can be more than one album
      item['album'].values.each{|val| 
        if !val.nil?
          if !mbid_hash["albummbid"].nil?
            #puts "album mbid found for: " + mbid_hash["albummbid"]
            @albums = Album.first_or_create({:albummbid => mbid_hash["albummbid"]},{:albummbid => mbid_hash["albummbid"], :albumname => item['album']['albumname'], :albumimage=>item['album']['albumimage']})
            @albums.save
            break
          else
            @albums = Album.first_or_create({:albumname => item['album']['albumname']},{:albumname => item['album']['albumname'], :albumimage=>item['album']['albumimage']})
            @albums.save
            break
          end
        end 
      }

      #Track
      #creating and saving track
      if !mbid_hash["trackmbid"].nil?        
        @tracks = Track.first_or_create({:trackmbid => mbid_hash["trackmbid"]},{:trackmbid => mbid_hash["trackmbid"],:title => item['title'],:show => item['show'],:talent => item['talent'],:aust => item['aust'],:tracklink => item['tracklink'],:tracknote => item['tracknote'],:duration => item['duration'],:publisher => item['publisher'],:datecopyrighted => item['datecopyrighted']})
        @tracks.save
      else
        @tracks = Track.first_or_create({:title => item['title'],:duration => item['duration']},{:title => item['title'],:show => item['show'],:talent => item['talent'],:aust => item['aust'],:tracklink => item['tracklink'],:tracknote => item['tracknote'],:duration => item['duration'],:publisher => item['publisher'],:datecopyrighted => item['datecopyrighted']})
        @tracks.save
      end
      
      #add the track to album - if album exists
      if !@albums.nil?
        @album_tracks = @albums.tracks << @tracks
        @album_tracks.save
      end
      
      #add the track to the artist
      @artist_tracks = @artist.tracks << @tracks
      @artist_tracks.save

      #adding play: only add if playedtime does not exsist in the database already
      play_items = Play.count(:playedtime=>item['playedtime'], :channel_id=>index)
      if play_items < 1
        @player = Play.new(:track_id =>@tracks.id, :channel_id => index, :playedtime=>item['playedtime'])
        @player.save
        @plays = @tracks.plays << @player
        @plays.save
      end  
      @artist.save
    end
  }
end

def mbid_lookup(artist, track, album)
  result_hash = {}
  
  #we can only hit mbrainz once a second so we sleep
  sleep 1
  
  q = MusicBrainz::Webservice::Query.new
  t_filter = MusicBrainz::Webservice::TrackFilter.new(:artist=>artist, :title=>track, :release=>album, :limit => 5)
  t_results = q.get_tracks(t_filter)
    
  #No results from the 'advanced' query, so trying artist and album individualy
  if t_results.count == 0

    #ARTIST
    q = MusicBrainz::Webservice::Query.new
    t_filter = MusicBrainz::Webservice::ArtistFilter.new(:name=>artist)
    t_results = q.get_artists(t_filter)
    if t_results.count > 0
      x = t_results.first
      if x.score == 100 && is_ascii(String(x.entity.name)) && String(x.entity.name).casecmp(artist)==0
        #puts 'ARTIST score: ' + String(x.score) + '- artist: ' + String(x.entity.name) + ' - artist mbid '+ String(x.entity.id.uuid)
        result_hash["artistmbid"] = String(x.entity.id.uuid)
      end
    end
    
    #ALBUM
    q = MusicBrainz::Webservice::Query.new
    t_filter = MusicBrainz::Webservice::ReleaseFilter.new(:artist=>artist, :title=>album)
    t_results = q.get_releases(t_filter)
    #puts "album results count "+t_results.count.to_s
    if t_results.count > 0    
      x = t_results.first
      #puts 'ALBUM score: ' + String(x.score) + '- artist: ' + String(x.entity.artist) + ' - artist mbid '+ String(x.entity.id.uuid) +' - release title '+ String(x.entity.title) + ' - orginal album title: '+album
      if x.score == 100 && is_ascii(String(x.entity.title)) && String(x.entity.title).casecmp(album)==0
        result_hash["albummbid"] = String(x.entity.id.uuid)
      end
    end
  
  elsif t_results.count > 0
    t_results.each{ |x|
      #puts 'score: ' + String(x.score) + '- artist: ' + String(x.entity.artist) + ' - artist mbid '+ String(x.entity.artist.id.uuid) + ' - track mbid: ' + String(x.entity.id.uuid) + ' - track: ' + String(x.entity.title)  +' - album: ' + String(x.entity.releases[0]) +' - album mbid: '+ String(x.entity.releases[0].id.uuid)
      if  x.score == 100 && is_ascii(String(x.entity.artist))
        result_hash["trackmbid"] = String(x.entity.id.uuid)
        result_hash["artistmbid"] = String(x.entity.artist.id.uuid)
        result_hash["albummbid"] = String(x.entity.releases[0].id.uuid)
      end
    }
  end
  return result_hash
end

def is_ascii(item)
  cd = CharDet.detect(item)
  encoding = cd['encoding']
  return encoding == 'ascii'
end

configure
parse