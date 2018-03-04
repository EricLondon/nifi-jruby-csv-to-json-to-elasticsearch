#!/usr/bin/env ruby

require 'csv'
require 'faker'

PRODUCT_COUNT = 1_000
ROWS_PER_FILE = 50
file_counter = 0
csv_data = []

people = (1..PRODUCT_COUNT).map do |id|
  if csv_data.size == 0
    csv_data << %w(id product_name price color material)
  end

  csv_data << [
    id,
    Faker::Commerce.product_name,
    Faker::Commerce.price,
    Faker::Commerce.color,
    Faker::Commerce.material
  ]

  # note: this does not check last iteration
  if csv_data.size > ROWS_PER_FILE
    file_counter += 1
    file_name = "products_part_#{file_counter}.csv"
    CSV.open("./#{file_name}", "wb") do |csv|
      csv_data.each do |c|
        csv << c
      end
    end
    csv_data = []
  end
end
