package src
// src.Direction.groovy

/**
 * Enum representing the direction for random date generation.
 */
enum Direction {
    PAST,
    FUTURE
}

/**
 * src.RandomDateGenerator
 *
 * A class to generate random java.util.Date objects either in the past or future
 * relative to the current date and time within a specified range.
 */
class RandomDateGenerator {

    // Instance of Random for generating random numbers
    private final Random random

    /**
     * Constructor to initialize the src.RandomDateGenerator.
     * Uses a single Random instance for efficiency.
     */
    RandomDateGenerator() {
        this.random = new Random()
    }

    /**
     * Generates a random Date object based on the specified direction and range.
     *
     * @param direction A src.Direction enum indicating the direction: PAST or FUTURE.
     * @param rangeInDays (Optional) The range in days to generate the random date.
     *                    Defaults to 36500 days (100 years) if not provided.
     * @return A new Date object representing the random date.
     * @throws IllegalArgumentException if the direction is null.
     */
    Date generateRandomDate(Direction direction, long rangeInDays = 36500L) {
        // Validate the direction parameter
        if (direction == null) {
            throw new IllegalArgumentException("src.Direction must not be null.")
        }

        // Current date and time
        Date now = new Date()

        // Convert range from days to milliseconds
        long rangeInMillis = rangeInDays * 24 * 60 * 60 * 1000

        Date startDate, endDate

        if (direction == Direction.PAST) {
            startDate = new Date(now.time - rangeInMillis)
            endDate = now
        } else { // direction == src.Direction.FUTURE
            startDate = now
            endDate = new Date(now.time + rangeInMillis)
        }

        // Calculate the difference in milliseconds between startDate and endDate
        long diffInMillis = endDate.time - startDate.time

        // Generate a random offset within the range
        long randomOffset = Math.abs(random.nextLong()) % (diffInMillis + 1)

        // Calculate the random timestamp
        long randomMillis = startDate.time + randomOffset

        // Return the new Date object
        return new Date(randomMillis)
    }
}