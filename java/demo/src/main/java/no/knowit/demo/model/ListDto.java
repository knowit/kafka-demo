package no.knowit.demo.model;

import lombok.Data;

import java.util.ArrayList;

@Data
public class ListDto {
    ArrayList<String> stringList;

    public ListDto() {
        stringList = new ArrayList<>();
    }
}
