package no.knowit.demo.model;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor(staticName = "of")
public class MailDto {
    String mail;
    String title;
}
