package no.knowit.demo.model;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.ArrayList;

@Data
@AllArgsConstructor(staticName = "of")
public class UserListPostInternal {
    PostDto post;
    ListDto userList;
}
