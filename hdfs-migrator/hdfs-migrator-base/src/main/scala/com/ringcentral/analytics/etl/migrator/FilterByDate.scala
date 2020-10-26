package com.ringcentral.analytics.etl.migrator

import java.time.LocalDate

class FilterByDate(predicate: LocalDate => Boolean) {
    def filter(date: LocalDate): Boolean = predicate(date)
}

object FilterByDate {
    def apply(startDate: String, endDate: String): FilterByDate = {
        new FilterByDate(evalFilter(startDate, endDate))
    }

    private def evalFilter(startDate: String, endDate: String): LocalDate => Boolean =
        (startDate, endDate) match {
            case ("", "") => _ => false
            case ("", endParam) =>
                val end = LocalDate.parse(endParam)
                date => date.isBefore(end)
            case (startParam, "") => date =>
                val start = LocalDate.parse(startParam)
                date.isAfter(start)
            case (startParam, endParam) =>
                val end = LocalDate.parse(endParam)
                val start = LocalDate.parse(startParam)
                date => date.isBefore(end) && date.isAfter(start)
        }
}
