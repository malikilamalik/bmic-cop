import csv
import random
import string
import sys
from datetime import datetime, timedelta, timezone


# ============================================================
# Configuration
# ============================================================

CUSTOMER_FILE = "customer.csv"

NUM_CUSTOMERS = 1000

MIN_AMOUNT = 10_000
MAX_AMOUNT = 100_000_000

TRANSACTION_TYPES = [
    "Debit",
    "Credit",
]

FIRST_NAMES = [
    "Andi",
    "Budi",
    "Citra",
    "Dewi",
    "Eko",
    "Fajar",
    "Gita",
    "Hendra",
    "Indah",
    "Joko",
    "Kurnia",
    "Lina",
    "Maya",
    "Nanda",
    "Putri",
    "Rian",
    "Sari",
    "Tono",
    "Umi",
    "Vina",
    "Wahyu",
    "Yudi",
    "Zahra",
]

LAST_NAMES = [
    "Pratama",
    "Saputra",
    "Wijaya",
    "Santoso",
    "Hidayat",
    "Nugroho",
    "Permana",
    "Setiawan",
    "Kusuma",
    "Ramadhan",
    "Siregar",
    "Gunawan",
    "Firmansyah",
    "Maulana",
    "Kurniawan",
]

DESCRIPTIONS = [
    "Pembayaran merchant menggunakan kartu debit",
    "Transfer dana antar rekening pelanggan",
    "Pembayaran tagihan bulanan pelanggan",
    "Pembelian produk melalui aplikasi digital",
    "Setoran tunai melalui jaringan perbankan",
    "Penarikan tunai melalui mesin ATM",
    "Pembayaran transaksi marketplace online",
    "Transfer masuk dari rekening pelanggan lain",
    "Pembayaran transaksi menggunakan QRIS",
    "Pengisian saldo rekening melalui transfer",
]


# ============================================================
# Customer
# ============================================================

def random_name():
    return (
        f"{random.choice(FIRST_NAMES)} "
        f"{random.choice(LAST_NAMES)}"
    )


def generate_account_number():
    """
    Generate an integer account number.

    Range:
        10,000,000,000
        -
        99,999,999,999
    """

    return random.randint(
        10_000_000_000,
        99_999_999_999,
    )


def generate_customers():

    print(
        f"Generating {NUM_CUSTOMERS:,} customers..."
    )

    with open(
        CUSTOMER_FILE,
        "w",
        newline="",
        encoding="utf-8",
    ) as file:

        writer = csv.writer(file)

        writer.writerow([
            "id",
            "nama",
            "no_rek",
        ])

        for customer_id in range(
            1,
            NUM_CUSTOMERS + 1,
        ):

            nama = random_name()

            no_rek = generate_account_number()

            writer.writerow([
                customer_id,
                nama,
                no_rek,
            ])

            # Progress every 100K customers
            if customer_id % 100_000 == 0:
                percentage = (
                    customer_id
                    / NUM_CUSTOMERS
                    * 100
                )

                print(
                    f"{customer_id:,} / "
                    f"{NUM_CUSTOMERS:,} "
                    f"({percentage:.1f}%)"
                )

    print()
    print("Customer generation complete.")
    print(f"Output: {CUSTOMER_FILE}")


# ============================================================
# Transaction distribution
# ============================================================

def random_transaction_count():
    """
    Distributed transaction count per customer/day.

    Approximate distribution:

        10% -> 0
        30% -> 1-2
        35% -> 3-5
        15% -> 6-10
         8% -> 11-30
         2% -> 31-100
    """

    roll = random.random()

    if roll < 0.10:
        return 0

    elif roll < 0.40:
        return random.randint(1, 2)

    elif roll < 0.75:
        return random.randint(3, 5)

    elif roll < 0.90:
        return random.randint(6, 10)

    elif roll < 0.98:
        return random.randint(11, 30)

    else:
        return random.randint(31, 100)


# ============================================================
# Transaction helpers
# ============================================================

def random_timestamp(date_str):

    date = datetime.strptime(
        date_str,
        "%Y%m%d",
    ).replace(
        tzinfo=timezone.utc
    )

    random_seconds = random.randint(
        0,
        24 * 60 * 60 - 1,
    )

    timestamp = (
        date
        + timedelta(seconds=random_seconds)
    )

    return timestamp.strftime(
        "%Y-%m-%dT%H:%M:%SZ"
    )


def random_amount():

    # Stored as cents.
    #
    # 5000000 = Rp50,000.00
    # 5000050 = Rp50,000.50

    return random.randint(
        MIN_AMOUNT,
        MAX_AMOUNT,
    )


def random_description(length=60):

    base = random.choice(
        DESCRIPTIONS
    )

    if len(base) >= length:
        return base[:length]

    # Add random characters until
    # the description is exactly 60 chars.

    chars = (
        string.ascii_letters
        + string.digits
        + " "
    )

    remaining = length - len(base)

    suffix = "".join(
        random.choice(chars)
        for _ in range(remaining)
    )

    return base + suffix


# ============================================================
# Transaction generator
# ============================================================

def generate_transactions(date_str):

    # --------------------------------------------------------
    # Validate customer file
    # --------------------------------------------------------

    try:
        customer_file = open(
            CUSTOMER_FILE,
            "r",
            encoding="utf-8",
        )
    except FileNotFoundError:

        print(
            f"ERROR: {CUSTOMER_FILE} not found."
        )

        print()
        print(
            "Generate customers first:"
        )

        print(
            "    python generate.py customer"
        )

        sys.exit(1)

    output_file = (
        f"transaction{date_str}.txt"
    )

    print(
        f"Generating transactions for {date_str}"
    )

    print(
        f"Output: {output_file}"
    )

    total_customers = 0
    total_transactions = 0

    distribution = {
        "0": 0,
        "1-2": 0,
        "3-5": 0,
        "6-10": 0,
        "11-30": 0,
        "31-100": 0,
    }

    transaction_id = 1

    with customer_file, open(
        output_file,
        "w",
        encoding="utf-8",
        buffering=1024 * 1024,
    ) as output:

        reader = csv.DictReader(
            customer_file
        )

        # ----------------------------------------------------
        # Header
        # ----------------------------------------------------

        output.write(
            "id,id_customer,timestamp,"
            "amount,transaction_type,description\n"
        )

        # ----------------------------------------------------
        # Read customer one-by-one
        # ----------------------------------------------------

        for row in reader:

            customer_id = int(
                row["id"]
            )

            total_customers += 1

            transaction_count = (
                random_transaction_count()
            )

            # Statistics
            if transaction_count == 0:
                distribution["0"] += 1

            elif transaction_count <= 2:
                distribution["1-2"] += 1

            elif transaction_count <= 5:
                distribution["3-5"] += 1

            elif transaction_count <= 10:
                distribution["6-10"] += 1

            elif transaction_count <= 30:
                distribution["11-30"] += 1

            else:
                distribution["31-100"] += 1

            # ------------------------------------------------
            # Generate transactions
            # ------------------------------------------------

            for _ in range(
                transaction_count
            ):

                timestamp = (
                    random_timestamp(
                        date_str
                    )
                )

                amount = random_amount()

                transaction_type = (
                    random.choice(
                        TRANSACTION_TYPES
                    )
                )

                description = (
                    random_description(60)
                )

                output.write(
                    f"{transaction_id},"
                    f"{customer_id},"
                    f"{timestamp},"
                    f"{amount},"
                    f"{transaction_type},"
                    f"{description}\n"
                )

                transaction_id += 1
                total_transactions += 1

            # Progress
            if (
                total_customers > 0
                and total_customers % 100_000 == 0
            ):

                print(
                    f"Processed "
                    f"{total_customers:,} "
                    f"customers | "
                    f"{total_transactions:,} "
                    f"transactions"
                )

    print()
    print(
        "Transaction generation complete."
    )

    print(
        f"Customers:    {total_customers:,}"
    )

    print(
        f"Transactions: {total_transactions:,}"
    )

    print(
        f"Output:       {output_file}"
    )

    print()
    print("Distribution:")
    print("-----------------------------")

    for category, count in distribution.items():

        percentage = (
            count
            / total_customers
            * 100
        )

        print(
            f"{category:>6}: "
            f"{count:>10,} customers "
            f"({percentage:5.2f}%)"
        )


# ============================================================
# Main
# ============================================================

def main():

    if len(sys.argv) < 2:

        print(
            "Usage:"
        )

        print(
            "  python generate.py customer"
        )

        print(
            "  python generate.py transaction YYYYMMDD"
        )

        print()
        print(
            "Examples:"
        )

        print(
            "  python generate.py customer"
        )

        print(
            "  python generate.py transaction 20260914"
        )

        sys.exit(1)

    command = sys.argv[1].lower()

    # --------------------------------------------------------
    # Generate customers
    # --------------------------------------------------------

    if command == "customer":

        generate_customers()

    # --------------------------------------------------------
    # Generate transactions
    # --------------------------------------------------------

    elif command == "transaction":

        if len(sys.argv) != 3:

            print(
                "Usage: "
                "python generate.py transaction YYYYMMDD"
            )

            sys.exit(1)

        date_str = sys.argv[2]

        try:

            datetime.strptime(
                date_str,
                "%Y%m%d",
            )

        except ValueError:

            print(
                "Invalid date. "
                "Expected YYYYMMDD."
            )

            sys.exit(1)

        generate_transactions(
            date_str
        )

    else:

        print(
            f"Unknown command: {command}"
        )

        sys.exit(1)


if __name__ == "__main__":
    main()
