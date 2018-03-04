#!/usr/bin/env ruby

require 'csv'
require 'faker'

PEOPLE_COUNT = 1_000
ROWS_PER_FILE = 50
file_counter = 0
csv_data = []

people = (1..PEOPLE_COUNT).map do |id|
  if csv_data.size == 0
    csv_data << %w(id first_name last_name email)
  end

  csv_data << [
    id,
    Faker::Name.first_name,
    Faker::Name.last_name,
    Faker::Internet.email
  ]

  # note: this does not check last iteration
  if csv_data.size > ROWS_PER_FILE
    file_counter += 1
    file_name = "people_part_#{file_counter}.csv"
    CSV.open("./#{file_name}", "wb") do |csv|
      csv_data.each do |c|
        csv << c
      end
    end
    csv_data = []
  end
end
