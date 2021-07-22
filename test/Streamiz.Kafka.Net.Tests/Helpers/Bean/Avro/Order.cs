namespace Streamiz.Kafka.Net.Tests.Helpers.Bean.Avro
{
	using global::Avro;
	using global::Avro.Specific;

	public partial class Order : ISpecificRecord
	{
		public static Schema _SCHEMA = Schema.Parse("{\"type\":\"record\",\"name\":\"Order\",\"namespace\":\"Streamiz.Kafka.Net.Tests.Helpers.Bean.Avro\",\"fields\":[{\"name\":\"or" +
				"der_id\",\"type\":\"int\"},{\"name\":\"price\",\"type\":\"float\"},{\"name\":\"product_id\",\"type" +
				"\":\"int\"}]}");
		private int _order_id;
		private float _price;
		private int _product_id;
		public virtual Schema Schema
		{
			get
			{
				return Order._SCHEMA;
			}
		}
		public int order_id
		{
			get
			{
				return this._order_id;
			}
			set
			{
				this._order_id = value;
			}
		}
		public float price
		{
			get
			{
				return this._price;
			}
			set
			{
				this._price = value;
			}
		}
		public int product_id
		{
			get
			{
				return this._product_id;
			}
			set
			{
				this._product_id = value;
			}
		}
		public virtual object Get(int fieldPos)
		{
			switch (fieldPos)
			{
				case 0: return this.order_id;
				case 1: return this.price;
				case 2: return this.product_id;
				default: throw new AvroRuntimeException("Bad index " + fieldPos + " in Get()");
			};
		}
		public virtual void Put(int fieldPos, object fieldValue)
		{
			switch (fieldPos)
			{
				case 0: this.order_id = (System.Int32)fieldValue; break;
				case 1: this.price = (System.Single)fieldValue; break;
				case 2: this.product_id = (System.Int32)fieldValue; break;
				default: throw new AvroRuntimeException("Bad index " + fieldPos + " in Put()");
			};
		}
	}
}
