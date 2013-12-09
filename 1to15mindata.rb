# 1to15minutedata.rb
# AJ Rossman - 12/8/13

# Program to convert data into regularly spaced interval data.  Timestamps are analyzed to determine
#  which 15 minute interval they belong to (1:00:01 to 1:15:00 are all included in 1:15:00 interval). 
#  The data are process by grouping all data into 15 minute intervals and then calculating average, 
#  minimum, maximum and number of measurements included in each interval.  The output data are formatted
#  Timestamp, average, min, max, # of measurements

# Program utilizes Redis key-value store.  The epoch seconds for each interval is used as a key and
#  lists are created for each interval.  Data for each timestamp are pushed to the key that 
#  corresponds to interval it belongs to.  After processing data, all data are dumped from each key
#  and processed into average, min, max and count.  

require 'csv'
require 'date'
require 'redis'

MINUTE_INTERVAL = 15  # This is the number of minutes in each interval
TIMESTAMP_COLUMN = 0  # This is the column with timestamp data

# create db => 3
redis_sorter = Redis.new(:db => 3)
# clean out redis db to start
redis_sorter.FLUSHDB

label_measurement = Array.new
column_measurement = Array.new
header = Array.new
processed_data = Array.new

# ask for csv filename
puts 'What is the name of the data file you are looking to process including suffix? (.csv supported)'
filename = gets.chomp
puts 'How many measurements do you want to process?'
num_meas = gets.chomp.to_i

(1..num_meas).each_with_index do |measurement, index|
	puts "What is the name of measurement #{measurement}?"
	label_measurement[index] = gets.chomp
	puts "What column is measurement_#{measurement} in ? (1st column is 0)"
	column_measurement[index] = gets.chomp.to_i  
end


# parse file line by line
CSV.foreach("#{filename}.csv", headers: true) do |row|
  puts '-------------------'
  timestamp = DateTime.strptime(row[TIMESTAMP_COLUMN], '%m/%d/%y %H:%M')
  es = timestamp.to_time.to_i - 60  # subtract 1 minute so :15 is in :15 interval and not :30

# determine 15 minute interval 
  es_interval = es / (MINUTE_INTERVAL * 60) + 1
  interval = Time.at(es_interval * MINUTE_INTERVAL * 60).to_datetime

  (1..num_meas).each_with_index do |measurement,index|
    puts "#{row[TIMESTAMP_COLUMN]} => #{interval} = #{row[column_measurement[index]]}"

    # add es_interval to set -> only adds unique values, so this will save only keys 
    redis_sorter.SADD('es_intervals',es_interval)
    # push data to list of appropriate key
    redis_sorter.LPUSH("#{es_interval}:#{label_measurement[index]}",row[column_measurement[index]]) 
  end
end


# Write data from Redis to file
CSV.open("#{filename}_processed.csv",'wb') do |csv|
  # headers
  header[0] = 'timestamp'
  (1..num_meas).each_with_index do |measurement, index|
    header.push("#{label_measurement[index]}_average","#{label_measurement[index]}_min","#{label_measurement[index]}_max","#{label_measurement[index]}_count")
  end
  csv << header
  # get all keys - probably should sort

  es_intervals_array = redis_sorter.SMEMBERS('es_intervals')

  # loop for each key for all measurements

  es_intervals_array.each do |es_interval|
  	processed_data = []
    interval = Time.at(es_interval.to_i * MINUTE_INTERVAL * 60).getlocal("+00:00").to_datetime
    processed_data[0] = interval
    (1..num_meas).each_with_index do |measurement, index|
      arr = redis_sorter.LRANGE("#{es_interval}:#{label_measurement[index]}",0,-1)
	  arrf = arr.collect{|i| i.to_f}
	  array_average = arrf.inject { |sum, el| sum + el } / arrf.size
	  processed_data.push(array_average, arr.min, arr.max, arr.count)
    end
    csv << processed_data
  end
end






