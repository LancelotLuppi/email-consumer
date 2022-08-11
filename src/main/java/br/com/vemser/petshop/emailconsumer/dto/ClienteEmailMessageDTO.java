package br.com.vemser.petshop.emailconsumer.dto;

import lombok.Data;

@Data
public class ClienteEmailMessageDTO {
    private Integer idCliente;
    private String nome;
    private String email;
}
