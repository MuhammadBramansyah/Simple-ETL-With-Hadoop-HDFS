---PROJECT DE 3

create table dwh_fact_orders (
	"userid" int not null,
	"orderdate" date not null,
	"quantity" int not null,
	"productname" varchar(255) not null,
	"productcategory" varchar(255) not null,
	"price" float not null,
	"salesamount" float not null,
	"PropertyState" varchar(255) not null,
	"PropertyCity" varchar(255) not null
    );

create table dwh_dim_users (
	"UserID" int not null,
	"UserSex" varchar(255) not null,
	"UserDevice" varchar(255) not null
    );

-- transformation query;

select a."UserID",
        a."OrderDate",
        a."Quantity",
        c."ProductName",
        c."ProductCategory",
        c."Price",
        b."PropertyCity",
        b."PropertyState",
        a."Quantity" * c."Price" as "SalesAmount"
from fact_orderdetails a 
left join dim_location b 
on a."PropertyID" = b."Prop ID"
left join dim_products c 
on a."ProductID" = c."ProductID"; 

