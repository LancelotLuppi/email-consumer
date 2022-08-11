package br.com.vemser.petshop.emailconsumer.dto;

import lombok.Data;

@Data
public class ClienteEmailMessageDTO {
    private Integer id;
    private String nome;
    private String email;
}
