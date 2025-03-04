struct DayOfWeek <: Dates.Period
    value::Int
end

allowedrange(::Type{P}) where {P <: Union{Second, Minute}} = P(0):P(59)
allowedrange(::Type{Hour}) = P(0):P(23)
allowedrange(::Type{Day}) = P(1):P(31)
allowedrange(::Type{Month}) = P(1):P(12)
allowedrange(::Type{DayOfWeek}) = P(0):P(6)

abstract type CronField end

struct Wildcard <: CronField end

allowed(::Period, ::Wildcard, ::Period, ::Wildcard) = true
allowed(p::Period, x::CronField, ::Period, ::Wildcard) = allowed(p, x)
allowed(::Period, ::Wildcard, q::Period, y::CronField) = allowed(q, y)
allowed(p::Period, x::CronField, q::Period, y::CronField) = allowed(p, x) || allowed(q, y)

allowed(p::Period, ::Wildcard) = true
nextallowed(p::Period, ::Wildcard) = p
minimumallowed(::Type{P}, ::Wildcard) where {P <: Union{Second, Minute, Hour}} = P(0)
minimumallowed(::Type{P}, ::Wildcard) where {P <: Union{Day, Month}} = P(1)

struct Numeric <: CronField
    value::Int
end

allowed(p::P, x::Numeric) where {P <: Period} = p == P(x.value)
nextallowed(_::P, x::Numeric) where {P <: Period} = P(x.value)
minimumallowed(::Type{P}, x::Numeric) where {P <: Period} = P(x.value)

struct Range <: CronField
    start::Int
    stop::Int
end
    
allowed(p::P, x::Range) where {P <: Period} = P(x.start) <= p <= P(x.stop)
nextallowed(p::P, x::Range) where {P <: Period} = allowed(p, x) ? p : P(x.start)
minimumallowed(::Type{P}, x::Range) where {P <: Period} = P(x.start)

struct Step <: CronField
    range::Union{Range, Wildcard}
    step::Int
end

function nextallowed(p::P, x::Step) where {P <: Period}
    #TODO
    if x.range isa Wildcard

    else

    end
end
minimumallowed(::Type{P}, x::Step) where {P <: Period} = minimumallowed(P, x.range)

struct List <: CronField
    values::Vector{Int}
    List(values::Vector{Int}) = new(sort!(values))
end

allowed(p::P, x::List) where {P <: Period} = any(==(P(p)), x.values)
function nextallowed(p::P, x::List) where {P <: Period}
    for v in x.values
        P(v) > p && return P(v)
    end
    return P(x.values[1])
end
minimumallowed(::Type{P}, x::List) where {P <: Period} = P(x.values[1])

struct Cron{S, M, H, D, Mo, DoW}
    second::S
    minute::M
    hour::H
    day_of_month::D
    month::Mo
    day_of_week::DoW
end

# parse an individual cron expression field
# looking for wildcard, numeric value, list, range, or step
function parseCronField(field)
    # wildcard
    if field == "*"
        return Wildcard()
    end
    # single numeric value
    if occursin(r"^\d+$", field)
        return Numeric(parse(Int, field))
    end
    # range
    if occursin(r"^\d+(-\d+)?$", field)
        parts = split(field, "-")
        if length(parts) == 1
            return Numeric(parse(Int, parts[1]))
        else
            start = parse(Int, parts[1])
            stop = parse(Int, parts[2])
            start > stop && throw(ArgumentError("Invalid range: $field"))
            return Range(start, stop)
        end
    end
    # step
    if occursin(r"^(?:\*|\d+-\d+)(?:/\d+)$", field)
        error("not yet supported")
        # parts = split(field, "/")
        # return Step(parseCronField(parts[1]), parse(Int, parts[2]))
    end
    # list
    if occursin(r"^\d+(,\d+)+$", field)
        return List(parse.(Int, split(field, ",")))
    end
    throw(ArgumentError("Invalid cron field: $field"))
end

function parseCron(cron::String)
    parts = split(cron, " ")
    if length(parts) == 5
        minute, hour, day, month, day_of_week = map(parseCronField, parts)
        second = Numeric(0)
    elseif length(parts) == 6
        second, minute, hour, day, month, day_of_week = map(parseCronField, parts)
    else
        throw(ArgumentError("Invalid cron expression: $cron"))
    end
    # validate cron fields (seconds in range 0-59, minutes in range 0-59, hours in range 0-23, day of month in range 1-31, month in range 1-12, day of week in range 0-6)
    if !(second isa Wildcard || (second isa Numeric && 0 <= second.value < 60))
        throw(ArgumentError("Invalid seconds value: $second"))
    end
    if !(minute isa Wildcard || (minute isa Numeric && 0 <= minute.value < 60))
        throw(ArgumentError("Invalid minutes value: $minute"))
    end
    if !(hour isa Wildcard || (hour isa Numeric && 0 <= hour.value < 24))
        throw(ArgumentError("Invalid hours value: $hour"))
    end
    if !(day_of_week isa Wildcard || (day_of_week isa Numeric && 0 <= day_of_week.value < 7))
        throw(ArgumentError("Invalid day of week value: $day_of_week"))
    end
    if !(day isa Wildcard || (day isa Numeric && 1 <= day.value <= 31))
        throw(ArgumentError("Invalid day of month value: $day"))
    end
    if !(month isa Wildcard || (month isa Numeric && 1 <= month.value <= 12))
        throw(ArgumentError("Invalid month value: $month"))
    end
    return Cron(second, minute, hour, day, month, day_of_week)
end

# Compute the next trigger time after 'from'
function getnext(cron::Cron, from::DateTime=Dates.now(UTC))
    minSeconds = minimumallowed(Second, cron.second)
    minMinutes = minimumallowed(Minute, cron.minute)
    minHours = minimumallowed(Hour, cron.hour)
    minDays = minimumallowed(Day, cron.day_of_month)
    next = from + Dates.Second(1)
    while true
        # check month
        curMonth = Month(next)
        if !allowed(curMonth, cron.month)
            # find next allowed month
            nextMonth = nextallowed(curMonth, cron.month)
            if nextMonth <= curMonth
                # if the next allowed month is less than the current month, we need to advance to the next year
                # then we want to use the minimum allowed month, day, hour, minute, and second
                next = DateTime(Year(next) + Year(1), nextMonth, minDays, minHours, minMinutes, minSeconds)
            elseif nextMonth > curMonth
                # if the next allowed month is greater than the current month, we need to reset
                # the day, hour, minute, and second to the minimum allowed values
                next = DateTime(Year(next), nextMonth, minDays, minHours, minMinutes, minSeconds)
            end
            continue
        end
        # check day
        curDay = Day(next)
        curDayOfWeek = DayOfWeek(dayofweek(next))
        if !allowed(curDay, cron.day_of_month, curDayOfWeek, cron.day_of_week)
            # find next allowed day
            nextDay = nextallowed(curDay, cron.day_of_month)
            if nextDay <= curDay
                # if the next allowed day is less than the current day, we need to advance to the next month
                # then we want to use the minimum allowed day, hour, minute, and second
                nextDayDate = DateTime(Year(next), Month(next) + Month(1), nextDay, minHours, minMinutes, minSeconds)
            elseif nextDay > curDay
                # if the next allowed day is greater than the current day, we need to reset
                # the hour, minute, and second to the minimum allowed values
                nextDayDate = DateTime(Year(next), Month(next), nextDay, minHours, minMinutes, minSeconds)
            end
            nextDayOfWeek = nextallowed(curDayOfWeek, cron.day_of_week)
            if nextDayOfWeek <= curDayOfWeek
                # if the next allowed day of week is less than the current day of week, we need to advance to the next week
                # then we want to use the minimum allowed day of week, hour, minute, and second
                nextDayOfWeekDate = next + Dates.Day(7 - curDayOfWeek.value + nextDayOfWeek.value)
                nextDayOfWeekDate = DateTime(Year(nextDayOfWeekDate), Month(nextDayOfWeekDate), Day(nextDayOfWeekDate), minHours, minMinutes, minSeconds)
            elseif nextDayOfWeek > curDayOfWeek
                # if the next allowed day of week is greater than the current day of week, we need to reset
                # the hour, minute, and second to the minimum allowed values
                nextdayOfWeekDate = next + Dates.Day(nextDayOfWeek.value - curDayOfWeek.value)
                nextDayOfWeekDate = DateTime(Year(nextDayOfWeekDate), Month(nextDayOfWeekDate), Day(nextDayOfWeekDate), minHours, minMinutes, minSeconds)
            end
            next = min(nextDayDate, nextDayOfWeekDate)
            continue
        end
        # check hour
        curHour = Hour(next)
        if !allowed(curHour, cron.hour)
            # find next allowed hour
            nextHour = nextallowed(curHour, cron.hour)
            if nextHour <= curHour
                # if the next allowed hour is less than the current hour, we need to advance to the next day
                # then we want to use the minimum allowed hour, minute, and second
                next = DateTime(Year(next), Month(next), Day(next) + Day(1), nextHour, minMinutes, minSeconds)
            elseif nextHour > curHour
                # if the next allowed hour is greater than the current hour, we need to reset
                # the minute and second to the minimum allowed values
                next = DateTime(Year(next), Month(next), Day(next), nextHour, minMinutes, minSeconds)
            end
            continue
        end
        # check minute
        curMinute = Minute(next)
        if !allowed(curMinute, cron.minute)
            # find next allowed minute
            nextMinute = nextallowed(curMinute, cron.minute)
            if nextMinute <= curMinute
                # if the next allowed minute is less than the current minute, we need to advance to the next hour
                # then we want to use the minimum allowed minute and second
                next = DateTime(Year(next), Month(next), Day(next), Hour(next) + Hour(1), nextMinute, minSeconds)
            elseif nextMinute > curMinute
                # if the next allowed minute is greater than the current minute, we need to reset
                # the second to the minimum allowed second
                next = DateTime(Year(next), Month(next), Day(next), Hour(next), nextMinute, minSeconds)
            end
            continue
        end
        # check second
        curSecond = Second(next)
        if !allowed(curSecond, cron.second)
            # find next allowed second
            nextSecond = nextallowed(curSecond, cron.second)
            if nextSecond <= curSecond
                # if the next allowed second is less than the current second, we need to advance to the next minute
                # then we want to use the minimum allowed second
                next = DateTime(Year(next), Month(next), Day(next), Hour(next), Minute(next) + Minute(1), nextSecond)
            elseif nextSecond > curSecond
                # if the next allowed second is greater than the current second, we need to reset
                # the second to the minimum allowed second
                next = DateTime(Year(next), Month(next), Day(next), Hour(next), Minute(next), nextSecond)
            end
            continue
        end
        # all fields are now valid/allowed, return
        return trunc(next, Second)
    end
end